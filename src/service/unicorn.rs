use futures::{Future, Stream};
use futures::sync::mpsc;

use {Error, Sender, Service};
use dispatch::{Streaming, StreamingDispatch};

pub type Version = i64;

#[derive(Debug)]
pub struct Close {
    sender: Sender,
}

impl Drop for Close {
    fn drop(&mut self) {
        self.sender.send(0, &[0; 0]);
    }
}

/// Wraps a `Service`, providing a convenient interface to the Unicorn service.
#[derive(Debug)]
pub struct Unicorn {
    service: Service,
}

impl Unicorn {
    /// Construct a new `Unicorn` by wrapping the specified `Service`.
    ///
    /// A `Service` is meant to be properly configured to point at "unicorn" service. Violating
    /// this will result in various framing errors.
    pub fn new(service: Service) -> Self {
        Self { service: service }
    }

    /// Subscribes for children updates for the node at the specified path.
    ///
    /// This method returns a future, which can be split into a cancellation token and a stream,
    /// which will return the actual list of children on each child creation or deletion. Other
    /// operations, such as children mutation, are not the subject of this method.
    pub fn children_subscribe<'a>(&self, path: &'a str) ->
        impl Future<Item = (Close, impl Stream<Item = Streaming<(Version, Vec<String>)>, Error = Error> + 'a), Error = Error> + 'a
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        self.service.call(1, &[path], dispatch).and_then(|sender| {
            Ok((Close { sender: sender }, rx.map_err(|()| Error::Canceled)))
        })
    }
}
