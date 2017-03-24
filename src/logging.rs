//! Contains a logging service with helper macro to ease integration with the cloud logging system.

use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::thread::{self, JoinHandle};

use futures::{future, Future, Stream};
use futures::sync::mpsc;

use tokio_core::reactor::Core;

use Service;

enum Event {
    Write(Vec<u8>),
    Close,
}

struct Inner {
    tx: mpsc::UnboundedSender<Event>,
    thread: Option<JoinHandle<()>>,
}

impl Inner {
    fn new(tx: mpsc::UnboundedSender<Event>, rx: mpsc::UnboundedReceiver<Event>) -> Self {
        let thread = thread::spawn(move || {
            let mut core = Core::new().expect("failed to initialize logger event loop");
            let handle = core.handle();

            let service = Service::new("logging", &handle);

            let future = rx.and_then(|event| {
                match event {
                    Event::Write(buf) => {
                        service.call_mute_raw(0, buf).then(|tx| {
                            drop(tx);
                            Ok(())
                        }).boxed()
                    }
                    Event::Close => future::err(()).boxed()
                }
            });

            drop(core.run(future.fold(0, |acc, _v| future::ok(acc))));
        });

        Self { tx: tx, thread: Some(thread) }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.tx.send(Event::Close).unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}

/// Allowed severity levels.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Sev {
    Debug,
    Info,
    Warn,
    Error,
}

impl Into<isize> for Sev {
    fn into(self) -> isize {
        match self {
            Sev::Debug => 0,
            Sev::Info => 1,
            Sev::Warn => 2,
            Sev::Error => 3,
        }
    }
}

/// A RAII context manager for a logging service.
///
/// This is an entry point for a logging service in the cocaine.
///
/// The `LoggerContext` creates a separate thread where the real logging service with its event
/// loop lives, and that allows to process all logging events using single TCP connection. The
/// communication with the context is done using unbounded channel, what makes emitting logging
/// events just pack-and-send operation.
///
/// Note, that the context destruction triggers wait operation until all messages are flushed into
/// the socket.
///
/// To create the logger object itself, call [`create`][create] method, which accepts an optional
/// *source* parameter - a short description where a log event came from.
///
/// There is also possible to configure a simple severity filter using [`filter`][filter] method.
///
/// [create]: #method.create
/// [filter]: #method.filter
#[derive(Clone)]
pub struct LoggerContext {
    tx: mpsc::UnboundedSender<Event>,
    inner: Arc<Inner>,
    filter: Filter,
}

impl LoggerContext {
    /// Constructs a new logger context.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded();
        Self {
            tx: tx.clone(),
            inner: Arc::new(Inner::new(tx, rx)),
            filter: Filter { sev: Arc::new(AtomicIsize::new(0)) },
        }
    }

    /// Creates a new logger, that will log events with the given *source* argument.
    ///
    /// All loggers are associated with the context used to create them, i.e share a single
    /// underlying service and the filter.
    pub fn create<T>(&self, source: T) -> Logger
        where T: Into<Cow<'static, str>>
    {
        Logger {
            source: source.into(),
            parent: self.clone(),
        }
    }

    /// Returns a severity filtering handle.
    ///
    /// Changing the severity threshold will affect all loggers that were created using this
    /// context.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }
}

impl Debug for LoggerContext {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("LoggerContext")
            .field("filter", &self.filter.get())
            .finish()
    }
}

/// Logger allows to log events directly into the Cocaine Logging Service.
///
/// Meant to be used in conjunction with [`log!`][log!] macro.
///
/// [log!]: ../macro.log.html
#[derive(Debug)]
pub struct Logger {
    source: Cow<'static, str>,
    parent: LoggerContext,
}

impl Logger {
    /// Returns a logger name, that is used as a *source* parameter.
    pub fn name(&self) -> &str {
        &self.source
    }

    /// Returns an internally mutable filter handle.
    pub fn filter(&self) -> &Filter {
        &self.parent.filter
    }

    #[doc(hidden)]
    /// Emits a new already properly encoded logging event.
    ///
    /// # Warning
    ///
    /// Do not use this method directly, use [`log!`][log!] macro instead. Violating this rule may
    /// lead to repeatedly disconnection from the real logging service due to framing error. The
    /// reason - a `buf` argument must be properly encoded.
    ///
    /// [log!]: ../macro.log.html
    pub fn __emit(&self, buf: Vec<u8>) {
        self.parent.tx.send(Event::Write(buf)).unwrap();
    }
}

/// Severity filter handle.
#[derive(Clone, Debug)]
pub struct Filter {
    sev: Arc<AtomicIsize>,
}

impl Filter {
    /// Returns currently set severity threshold.
    pub fn get(&self) -> isize {
        self.sev.load(Ordering::Relaxed)
    }

    /// Sets the severity threshold.
    ///
    /// All logging events with severity less than the specified threshold will be dropped.
    pub fn set(&self, sev: isize) {
        self.sev.store(sev, Ordering::Relaxed)
    }
}

#[macro_export]
macro_rules! log (
    ($log:ident, $sev:expr, $fmt:expr, [$($args:tt)*], {$($name:ident: $val:expr,)+}) => {{
        extern crate rmp_serde as rmps;

        let sev: isize = $sev.into();

        if sev >= $log.filter().get() {
            let buf = rmps::to_vec(&(sev, $log.name(), format!($fmt, $($args)*), ($((stringify!($name), &$val)),+))).unwrap();
            $log.__emit(buf);
        }
    }};
    ($log:ident, $sev:expr, $fmt:expr, [$($args:tt)*], {}) => {{
        extern crate rmp_serde as rmps;

        let sev: isize = $sev.into();

        if sev >= $log.filter().get() {
            let buf = rmps::to_vec(&(sev, $log.name(), format!($fmt, $($args)*))).unwrap();
            $log.__emit(buf);
        }
    }};
    ($log:ident, $sev:expr, $fmt:expr, {$($name:ident: $val:expr,)*}) => {{
        log!($log, $sev, $fmt, [], {$($name: $val,)*})
    }};
    ($log:ident, $sev:expr, $fmt:expr, [$($args:tt)*]) => {{
        log!($log, $sev, $fmt, [$($args)*], {})
    }};
    ($log:ident, $sev:expr, $fmt:expr, $($args:tt)*) => {{
        log!($log, $sev, $fmt, [$($args)*], {})
    }};
    (I $log:ident, $fmt:expr, $($args:tt)*) => {{
        log!($log, Sev::Info, $fmt, [$($args)*], {})
    }};
    ($log:ident, $sev:expr, $fmt:expr) => {{
        log!($log, $sev, $fmt, [], {})
    }};
    (I $log:ident, $fmt:expr) => {{
        log!($log, Sev::Info, $fmt, [], {})
    }};
);
