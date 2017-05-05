use futures::{Future, Stream};
use futures::stream::{BoxStream};
use futures::sync::mpsc;

use rmpv::{self, Value};

use serde::Deserialize;

use {Error, Sender, Service};
use dispatch::StreamingDispatch;
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

enum Method {
    Subscribe,
    ChildrenSubscribe,
}

impl Into<u64> for Method {
    #[inline]
    fn into(self) -> u64 {
        match self {
            Method::Subscribe => 0,
            Method::ChildrenSubscribe => 1,
        }
    }
}

/// Wraps a `Service`, providing a convenient interface to the Unicorn service.
///
/// The `Unicorn` service is a Cloud Configuration Service. It provides an ability to save, read
/// and subscribe for your configuration updates in a strongly-consistent way. Thus all values
/// have some epoch number to match the version of the value obtained.
///
/// A typical use case is to load the configuration at application start-up. Another use case is to
/// subscribe for configuration updates to be able to be notified on its changes immediately
/// without explicit polling.
///
/// # Note
///
/// It's not recommended to use the `Unicorn` as a storage for large files, cluster states or
/// something else big enough - a typical node size must count in kilobytes at max.
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

    /// Subscribes for updates for the node at the specified path.
    ///
    /// This method returns a future, which can be split into a cancellation token and a stream of
    /// versioned node values. In addition it tries to convert received values into the specified
    /// `Deserialize` type.
    ///
    /// # Errors
    ///
    /// Any error occurred is transformed to a stream error (note, that until futures 0.2 errors
    /// are not force the stream to be closed).
    ///
    /// In addition to common errors this method also emits `Error::InvalidDataFraming` on failed
    /// attempt to deserialize the received value into the specified type.
    pub fn subscribe<T: for<'de> Deserialize<'de> + Send + 'static>(&self, path: String) ->
        impl Future<Item=(Close, BoxStream<(T, Version), Error>), Error=Error>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        self.service.call(Method::Subscribe.into(), &[path], Vec::new(), dispatch).and_then(|sender| {
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
    pub fn children_subscribe<'a>(&self, path: String) ->
        impl Future<Item=(Close, BoxStream<(Version, Vec<String>), Error>), Error=Error> + 'a
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        self.service.call(Method::ChildrenSubscribe.into(), &[path], Vec::new(), dispatch).and_then(|sender| {
            let handle = Close { sender: sender };
            let stream = rx.map_err(|()| Error::Canceled)
                .then(Flatten::flatten)
                .boxed();
            Ok((handle, stream))
        }).boxed()
    }
}
