use futures::{self, Future};
use futures::sync::mpsc::{self, UnboundedReceiver};

use rmpv::{self, ValueRef};

use {Dispatch, Error, Service};

struct AppDispatch {
    tx: mpsc::UnboundedSender<Streaming>,
}

impl Dispatch for AppDispatch {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        let result = match ty {
            0 => {
                match rmpv::ext::from_value(response.to_owned()) {
                    Ok((data,)) => {
                        Streaming::Write(data)
                    }
                    Err(err) => {
                        Streaming::Error(Error::InvalidDataFraming(format!("{}", err)))
                    }
                }
            }
            1 => {
                match rmpv::ext::from_value(response.to_owned()) {
                    Ok(((category, ty), description)) => {
                        Streaming::Error(Error::Service(category, ty, description))
                    }
                    Err(err) => {
                        Streaming::Error(Error::InvalidDataFraming(format!("{}", err)))
                    }
                }
            }
            2 => {
                #[derive(Deserialize)]
                struct Close;

                match rmpv::ext::from_value(response.to_owned()) {
                    Ok(Close) => {
                        Streaming::Close
                    }
                    Err(err) => {
                        Streaming::Error(Error::InvalidDataFraming(format!("{}", err)))
                    }
                }
            }
            m => {
                Streaming::Error(Error::InvalidDataFraming(format!("unexpected message with type {}", m)))
            }
        };

        let mut close = false;
        if let &Streaming::Error(..) = &result {
            close = true;
        }
        if let &Streaming::Close = &result {
            close = true;
        }

        drop(self.tx.send(result));

        if close {
            drop(self.tx);
            None
        } else {
            Some(self)
        }
    }

    fn discard(self: Box<Self>, err: &Error) {
        drop(self.tx.send(Streaming::Error(err.clone())))
    }
}

#[derive(Debug)]
pub enum Streaming {
    Write(String),
    Error(Error),
    Close,
}

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

pub struct App {
    service: Service,
}

impl App {
    pub fn new(service: Service) -> Self {
        App { service: service }
    }

    pub fn enqueue<'a>(&self, event: &'a str) ->
        impl Future<Item=(Sender, UnboundedReceiver<Streaming>), Error=Error> + 'a
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
