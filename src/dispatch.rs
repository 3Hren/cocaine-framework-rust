//! Contains helper dispatches that ease working with common protocols, like `Primitive` or
//! `Streaming`.

use futures::sync::{mpsc, oneshot};

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
/// either chunk, error or close events.
#[derive(Debug)]
pub struct StreamingDispatch<T> {
    tx: mpsc::UnboundedSender<Streaming<T>>,
}

impl<T> StreamingDispatch<T> {
    /// Constructs a `StreamingDispatch` by wrapping the specified sender.
    pub fn new(tx: mpsc::UnboundedSender<Streaming<T>>) -> Self {
        Self { tx: tx }
    }
}

impl<T: for<'de> Deserialize<'de> + Send + 'static> Dispatch for StreamingDispatch<T> {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        let mut recurse = true;
        let result = match protocol::deserialize::<protocol::Streaming<T>>(ty, response)
            .flatten()
        {
            Ok(Some(data)) => Streaming::Write(data),
            Ok(None) => {
                recurse = false;
                Streaming::Close
            }
            Err(err) => {
                recurse = false;
                Streaming::Error(err)
            }
        };

        if self.tx.send(result).is_err() {
            return None;
        }

        if recurse { Some(self) } else { None }
    }

    fn discard(self: Box<Self>, err: &Error) {
        // TODO: Should we need to close the stream?
        drop(self.tx.send(Streaming::Error(err.clone())))
    }
}

#[derive(Debug)]
pub struct StreamingDispatch02<T> {
    tx: mpsc::UnboundedSender<Result<T, Error>>,
}

impl<T> StreamingDispatch02<T> {
    /// Constructs a `StreamingDispatch` by wrapping the specified sender.
    pub fn new(tx: mpsc::UnboundedSender<Result<T, Error>>) -> Self {
        Self { tx: tx }
    }
}

impl<T: for<'de> Deserialize<'de> + Send + 'static> Dispatch for StreamingDispatch02<T> {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        match protocol::deserialize::<protocol::Streaming<T>>(ty, response).flatten() {
            Ok(Some(data)) => {
                if self.tx.send(Ok(data)).is_ok() {
                    Some(self)
                } else {
                    None
                }
            },
            Err(err) => {
                if self.tx.send(Err(err)).is_ok() {
                    Some(self)
                } else {
                    None
                }
            }
            Ok(None) => None,
        }
    }

    fn discard(self: Box<Self>, err: &Error) {
        drop(self.tx.send(Err(err.clone())))
    }
}
