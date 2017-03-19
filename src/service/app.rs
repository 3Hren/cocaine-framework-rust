use futures::{self, Future};
use futures::sync::mpsc::{self, UnboundedReceiver};

use rmpv::ValueRef;

use {Dispatch, Error, Service};
use protocol::{self, Flatten};

#[derive(Debug)]
struct AppDispatch {
    tx: mpsc::UnboundedSender<Streaming<String>>,
}

impl Dispatch for AppDispatch {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        let mut recurse = true;
        let result = match protocol::deserialize::<protocol::Streaming<String>>(ty, response)
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

        drop(self.tx.send(result));

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

#[derive(Debug)]
pub enum Streaming<T> {
    Write(T),
    Error(Error),
    Close,
}

#[derive(Debug)]
pub struct Sender {
    inner: super::super::Sender,
}

impl Sender {
    pub fn write(&self, data: &str) {
        self.inner.send(0, &[data]);
    }

    pub fn error(self, id: u64, description: &str) {
        self.inner.send(1, &((0, id), description));
    }

    pub fn close(self) {
        self.inner.send(2, &[0; 0]);
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.inner.send(2, &[0; 0]);
    }
}

#[derive(Debug)]
pub struct App {
    service: Service,
}

impl App {
    pub fn new(service: Service) -> Self {
        Self { service: service }
    }

    pub fn enqueue<'a>(&self, event: &'a str) ->
        impl Future<Item=(Sender, UnboundedReceiver<Streaming<String>>), Error=Error> + 'a
    {
        let (tx, rx) = mpsc::unbounded();

        let dispatch = AppDispatch { tx: tx };

        self.service.call(0, &[event], dispatch)
            .and_then(|sender| {
                let sender = Sender { inner: sender };
                futures::future::ok((sender, rx))
            })
    }
}
