use futures::{Future, Stream};
use futures::sync::mpsc;

use {Error, Sender, Service};
use dispatch::{Streaming, StreamingDispatch};

/// A value version.
///
/// Unicorn is a strongly-consistent system and requires a some epoch-value to be incremented each
/// time a mutation action occurs.
pub type Version = i64;

/// A close handle for some `Unicorn` events.
///
/// Some streams are required to be closed to cancel the operation, for example to unlock the node
/// or to unsubscribe from notifications, otherwise a resource can leak.
/// This handle does it automatically on destruction. To close the channel manually use `drop`
/// function from the standard library.
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
