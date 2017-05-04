use futures::{Future, Stream};
use futures::future::{BoxFuture};
use futures::stream::{BoxStream};
use futures::sync::mpsc;

use rmpv::{self, Value};

use serde::Deserialize;

use {Error, Sender, Service};
use dispatch::StreamingDispatch02;
use protocol::Flatten;

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
#[derive(Clone, Debug)]
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

    pub fn subscribe<T: for<'de> Deserialize<'de> + Send + 'static>(&self, path: String) ->
        impl Future<Item=(Close, BoxStream<(T, Version), Error>), Error=Error>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch02::new(tx);
        self.service.call(0, &[path], dispatch).and_then(|sender| {
            let handle = Close { sender: sender };
            let stream = rx.map_err(|()| Error::Canceled)
                .then(Flatten::flatten)
                .and_then(|(val, version): (Value, Version)| {
                    match rmpv::ext::deserialize_from(val) {
                        Ok(val) => Ok((val, version)),
                        Err(err) => Err(Error::InvalidDataFraming(err.to_string())),
                    }
                })
                .boxed();

            Ok((handle, stream))
        })
    }

    /// Subscribes for children updates for the node at the specified path.
    ///
    /// This method returns a future, which can be split into a cancellation token and a stream,
    /// which will return the actual list of children on each child creation or deletion. Other
    /// operations, such as children mutation, are not the subject of this method.
    pub fn children_subscribe(&self, path: String) ->
//        impl Future<Item = (Close, impl Stream<Item = (Version, Vec<String>), Error = Error> + 'a), Error = Error> + 'a
        BoxFuture<(Close, BoxStream<(Version, Vec<String>), Error>), Error>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch02::new(tx);
        self.service.call(1, &[path], dispatch).and_then(|sender| {
            Ok((Close { sender: sender }, rx.map_err(|()| Error::Canceled).then(Flatten::flatten).boxed()))
        }).boxed()
    }
}
