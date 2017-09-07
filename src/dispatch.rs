//! Contains helper dispatches that ease working with common protocols, like `Primitive` or
//! `Streaming`.

use futures::Future;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;

use serde::Deserialize;

use rmpv::ValueRef;

use {Dispatch, Error};
use protocol::{self, Flatten, Primitive};

/// A single-shot dispatch wraps the given oneshot sender and implements `Primitive` protocol
/// emitting either value or error.
///
/// The majority of services adheres such protocol.
#[derive(Debug)]
pub struct PrimitiveDispatch<T> {
    tx: oneshot::Sender<Result<T, Error>>,
}

impl<T> PrimitiveDispatch<T> {
    /// Constructs a `PrimitiveDispatch` by wrapping the specified oneshot sender.
    pub fn new(tx: oneshot::Sender<Result<T, Error>>) -> Self {
        Self { tx: tx }
    }

    /// Constructs a `PrimitiveDispatch` paired with a future of result.
    ///
    /// The future returned will be resolved at a time when an incoming message is consumed by the
    /// dispatch.
    pub fn pair() -> (Self, impl Future<Item = T, Error = Error>) {
        let (tx, rx) = oneshot::channel();
        let result = PrimitiveDispatch::new(tx);
        let future = rx.map_err(|e| e.into()).then(|r| r.and_then(|v| v));

        (result, future)
    }
}

impl<T: for<'de> Deserialize<'de> + Send> Dispatch for PrimitiveDispatch<T> {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        let result = protocol::deserialize::<Primitive<T>>(ty, response)
            .flatten();
        drop(self.tx.send(result));

        None
    }

    fn discard(self: Box<Self>, err: &Error) {
        drop(self.tx.send(Err(err.clone())));
    }
}

#[derive(Debug)]
pub enum Streaming<T> {
    Write(T),
    Error(Error),
    Close,
}

/// A streaming dispatch wraps the given stream and implements `Streaming` protocol, emitting
/// either chunk, error or close events as usual stream events.
#[derive(Debug)]
pub struct StreamingDispatch<T> {
    tx: UnboundedSender<Result<T, Error>>,
}

impl<T> StreamingDispatch<T> {
    /// Constructs a `StreamingDispatch` by wrapping the specified sender.
    pub fn new(tx: UnboundedSender<Result<T, Error>>) -> Self {
        Self { tx: tx }
    }

    fn send(self: Box<Self>, result: Result<T, Error>) -> Option<Box<Dispatch>>
        where T: for<'de> Deserialize<'de> + Send + 'static
    {
        if self.tx.unbounded_send(result).is_ok() {
            Some(self)
        } else {
            None
        }
    }
}

impl<T: for<'de> Deserialize<'de> + Send + 'static> Dispatch for StreamingDispatch<T> {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        match protocol::deserialize::<protocol::Streaming<T>>(ty, response).flatten() {
            Ok(Some(data)) => self.send(Ok(data)),
            Ok(None) => None,
            Err(err) => self.send(Err(err)),
        }
    }

    fn discard(self: Box<Self>, err: &Error) {
        drop(self.tx.unbounded_send(Err(err.clone())))
    }
}
