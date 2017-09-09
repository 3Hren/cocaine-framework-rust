//! Application service API.

use futures::{self, Future};
use futures::sync::mpsc::{self, UnboundedReceiver};

use {Error, Request, Service};
use dispatch::StreamingDispatch;

/// Application service stream sender.
#[derive(Debug)]
pub struct Sender {
    inner: super::super::Sender,
}

impl Sender {
    /// Writes a chunk of data to the stream.
    pub fn write(&self, data: &str) {
        self.inner.send(Request::new(0, &[data]).unwrap());
    }

    /// Sends an error to the stream, closing it.
    pub fn error(self, id: u64, description: &str) {
        self.inner.send(Request::new(1, &((0, id), description)).unwrap());
    }

    /// Closes the stream.
    pub fn close(self) {}
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.inner.send(Request::new(2, &[0; 0]).unwrap());
    }
}

/// Application service wrapper.
#[derive(Debug)]
pub struct App {
    service: Service,
}

impl App {
    /// Constructs an application service wrapper using the specified service.
    pub fn new(service: Service) -> Self {
        Self { service }
    }

    /// Enqueues an event, opening a bidirectional stream on success.
    pub fn enqueue(&self, event: &str) ->
        impl Future<Item = (Sender, UnboundedReceiver<Result<String, Error>>), Error = Error>
    {
        let (tx, rx) = mpsc::unbounded();

        let dispatch = StreamingDispatch::new(tx);

        self.service.call(Request::new(0, &[event]).unwrap(), dispatch)
            .and_then(|sender| {
                let sender = Sender { inner: sender };
                futures::future::ok((sender, rx))
            })
    }
}
