use futures::{self, Future};
use futures::sync::mpsc::{self, UnboundedReceiver};

use {Error, Service};
use dispatch::StreamingDispatch;

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

    pub fn close(self) {}
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.inner.send(2, &[0; 0]);
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
        Self { service: service }
    }

    pub fn enqueue<'a>(&self, event: &'a str) ->
        impl Future<Item = (Sender, UnboundedReceiver<Result<String, Error>>), Error = Error> + 'a
    {
        let (tx, rx) = mpsc::unbounded();

        let dispatch = StreamingDispatch::new(tx);

        self.service.call(0, &[event], Vec::new(), dispatch)
            .and_then(|sender| {
                let sender = Sender { inner: sender };
                futures::future::ok((sender, rx))
            })
    }
}
