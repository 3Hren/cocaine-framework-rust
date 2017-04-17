//! Contains helper dispatches that ease working with common protocols, like `Primitive` or
//! `Streaming`.

use futures::sync::mpsc;

use serde::Deserialize;

use rmpv::ValueRef;

use {Dispatch, Error};
use protocol::{self, Flatten};

#[derive(Debug)]
pub enum Streaming<T> {
    Write(T),
    Error(Error),
    Close,
}

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
