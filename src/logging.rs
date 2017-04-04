//! Contains a logging service with helper macro to ease integration with the cloud logging system.

use std::borrow::Cow;
use std::error;
use std::fmt::{self, Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::thread::{self, JoinHandle};

use futures::{future, Future, Stream};
use futures::sync::mpsc;

use tokio_core::reactor::Core;

use Service;

const DEFAULT_LOGGING_NAME: &str = "logging";

enum Event {
    Write(Vec<u8>),
    Close,
}

struct Inner {
    tx: mpsc::UnboundedSender<Event>,
    thread: Option<JoinHandle<()>>,
}

impl Inner {
    fn new(name: Cow<'static, str>, tx: mpsc::UnboundedSender<Event>, rx: mpsc::UnboundedReceiver<Event>) -> Self {
        let thread = thread::spawn(move || {
            let mut core = Core::new().expect("failed to initialize logger event loop");
            let handle = core.handle();

            let service = Service::new(name, &handle);

            let future = rx.and_then(|event| {
                match event {
                    Event::Write(buf) => {
                        // TODO: For unknown reasons this one hangs until external reconnection
                        // after sending some messages.
//                        service.call_mute_raw(0, buf).then(|tx| {
//                            drop(tx);
//                            Ok(())
//                        }).boxed()
                        service.call_mute_raw(0, buf);
                        future::ok(()).boxed()
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
pub enum Severity {
    Debug,
    Info,
    Warn,
    Error,
}

impl Into<isize> for Severity {
    fn into(self) -> isize {
        match self {
            Severity::Debug => 0,
            Severity::Info => 1,
            Severity::Warn => 2,
            Severity::Error => 3,
        }
    }
}

impl Display for Severity {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let result = match *self {
            Severity::Debug => "debug",
            Severity::Info => "info",
            Severity::Warn => "warn",
            Severity::Error => "error",
        };

        fmt.write_str(result)
    }
}

/// An error returned when parsing a severity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeverityParseError;

impl Display for SeverityParseError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt.write_str(error::Error::description(self))
    }
}

impl error::Error for SeverityParseError {
    fn description(&self) -> &str {
        "invalid severity syntax"
    }
}

impl FromStr for Severity {
    type Err = SeverityParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "debug" => Ok(Severity::Debug),
            "info" => Ok(Severity::Info),
            "warn" | "warning" => Ok(Severity::Warn),
            "error" => Ok(Severity::Error),
            _ => Err(SeverityParseError),
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
    name: Cow<'static, str>,
    inner: Arc<Inner>,
    filter: Filter,
}

impl LoggerContext {
    /// Constructs a new logger context with the given name, that is used as a logging service's
    /// name.
    ///
    /// # Warning
    ///
    /// Beware of connecting to a service, which name is just occasionally equals with the specified
    /// one. Doing so will probably lead to reconnection after each request because of framing
    /// errors.
    ///
    /// # Examples
    ///
    /// ```
    /// use cocaine::logging::LoggerContext;
    ///
    /// let log = LoggerContext::default();
    /// assert_eq!("logging", log.name());
    ///
    /// let log = LoggerContext::new("logging::v2");
    /// assert_eq!("logging::v2", log.name());
    /// ```
    pub fn new<N>(name: N) -> Self
        where N: Into<Cow<'static, str>>
    {
        let name = name.into();

        let (tx, rx) = mpsc::unbounded();
        Self {
            tx: tx.clone(),
            name: name.clone(),
            inner: Arc::new(Inner::new(name, tx, rx)),
            filter: Filter { sev: Arc::new(AtomicIsize::new(0)) },
        }
    }

    /// Returns the associated logging service's name given at construction time.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Creates a new logger, that will log events with the given *source* argument.
    ///
    /// All loggers are associated with the context used to create them, i.e share a single
    /// underlying service and the filter.
    pub fn create<T>(&self, source: T) -> Logger
        where T: Into<Cow<'static, str>>
    {
        Logger {
            parent: self.clone(),
            source: source.into(),
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

impl Default for LoggerContext {
    fn default() -> Self {
        LoggerContext::new(DEFAULT_LOGGING_NAME)
    }
}

impl Debug for LoggerContext {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("LoggerContext")
            .field("name", &self.name)
            .field("filter", &self.filter.get())
            .finish()
    }
}

/// Logger allows to log events directly into the Cocaine Logging Service.
///
/// Meant to be used in conjunction with [`log!`][log!] macro.
///
/// [log!]: ../macro.log.html
#[derive(Debug, Clone)]
pub struct Logger {
    parent: LoggerContext,
    source: Cow<'static, str>,
}

impl Logger {
    /// Returns the associated logging service's name given at construction time.
    pub fn name(&self) -> &str {
        self.parent.name()
    }

    /// Returns a logger name, that is used as a *source* parameter.
    pub fn source(&self) -> &str {
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
    /// reason is - a `buf` argument must be properly encoded.
    ///
    /// [log!]: ../macro.log.html
    pub fn __emit(&self, buf: Vec<u8>) {
        self.parent.tx.send(Event::Write(buf)).unwrap();
    }
}

/// Severity filter handle.
///
/// A `Filter` allows to configure the severity level threshold for logging events. All events with
/// severity less than the specified will be rejected immediately.
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
            let buf = rmps::to_vec(&(sev, $log.source(), format!($fmt, $($args)*), ($((stringify!($name), &$val)),+))).unwrap();
            $log.__emit(buf);
        }
    }};
    ($log:ident, $sev:expr, $fmt:expr, [$($args:tt)*], {}) => {{
        extern crate rmp_serde as rmps;

        let sev: isize = $sev.into();

        if sev >= $log.filter().get() {
            let buf = rmps::to_vec(&(sev, $log.source(), format!($fmt, $($args)*))).unwrap();
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
);
