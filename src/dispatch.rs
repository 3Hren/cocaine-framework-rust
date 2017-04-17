//! Contains helper dispatches that ease working with common protocols, like `Primitive` or
//! `Streaming`.

use futures::sync::{oneshot, mpsc};

use serde::Deserialize;

use rmpv::ValueRef;

use {Dispatch, Error};
use protocol::{self, Flatten, Primitive};

/// A single-shot dispatch that implements primitive protocol and emits either value or error.
#[derive(Debug)]
pub struct PrimitiveDispatch<T> {
    tx: oneshot::Sender<Result<T, Error>>,
}

impl<T> PrimitiveDispatch<T> {
    pub fn new(tx: oneshot::Sender<Result<T, Error>>) -> Self {
        Self { tx: tx }
    }
}

impl<T: Deserialize + Send> Dispatch for PrimitiveDispatch<T> {
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

///
#[derive(Debug)]
pub struct StreamingDispatch<T> {
    tx: mpsc::UnboundedSender<Streaming<T>>,
}

impl<T> StreamingDispatch<T> {
    pub fn new(tx: mpsc::UnboundedSender<Streaming<T>>) -> Self {
        Self { tx: tx }
    }
}

impl<T: Deserialize + Send + 'static> Dispatch for StreamingDispatch<T> {
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

        if recurse {
            Some(self)
        } else {
            None
        }
    }

    fn discard(self: Box<Self>, err: &Error) {
        drop(self.tx.send(Streaming::Error(err.clone())))
    }
}
