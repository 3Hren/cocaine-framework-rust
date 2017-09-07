use futures::{Future, Stream};
use futures::sync::mpsc;

use rmpv::{self, Value};

use serde::Deserialize;

use {Error, Request, Sender, Service};
use dispatch::{PrimitiveDispatch, StreamingDispatch};
use hpack::RawHeader;
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

impl Close {
    pub fn close(self) {}
}

impl Drop for Close {
    fn drop(&mut self) {
        self.sender.send(Request::new(0, &[0; 0]).unwrap());
    }
}

enum Method {
    Subscribe,
    ChildrenSubscribe,
    Get,
}

impl Into<u64> for Method {
    #[inline]
    fn into(self) -> u64 {
        match self {
            Method::Subscribe => 0,
            Method::ChildrenSubscribe => 1,
            Method::Get => 3,
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
        Self { service }
    }

    /// Obtains a value with its version stored at specified path.
    ///
    /// This method returns a future with specified `Deserialize` type.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cocaine::{Core, Service};
    /// use cocaine::service::Unicorn;
    ///
    /// let mut core = Core::new().unwrap();
    /// let unicorn = Unicorn::new(Service::new("unicorn", &core.handle()));
    ///
    /// let future = unicorn.get("/cocaine/config");
    ///
    /// let (value, version): (Option<String>, i64) = core.run(future).unwrap();
    /// ```
    pub fn get<T>(&self, path: &str) ->
        impl Future<Item=(Option<T>, Version), Error=Error>
    where
        T: for<'de> Deserialize<'de>
    {
        let (dispatch, future) = PrimitiveDispatch::pair();
        self.service.call(Request::new(Method::Get.into(), &[path]).unwrap(), dispatch);

        future.and_then(|(val, version): (Value, Version)| {
            match rmpv::ext::deserialize_from(val) {
                Ok(val) => Ok((val, version)),
                Err(err) => Err(Error::InvalidDataFraming(err.to_string())),
            }
        })
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
    pub fn subscribe<T, H>(&self, path: &str, headers: H) ->
        impl Future<Item=(Close, Box<Stream<Item=(Option<T>, Version), Error=Error> + Send>), Error=Error>
    where
        T: for<'de> Deserialize<'de> + Send + 'static,
        H: Into<Option<Vec<RawHeader>>>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        let headers = headers.into().unwrap_or_default();
        let request = Request::new(Method::Subscribe.into(), &[path]).unwrap()
            .add_headers(headers);
        self.service.call(request, dispatch).and_then(|sender| {
            let handle = Close { sender: sender };
            let stream = box rx.map_err(|()| Error::Canceled)
                .then(Flatten::flatten)
                .and_then(|(val, version): (Value, Version)| {
                    match rmpv::ext::deserialize_from(val) {
                        Ok(val) => Ok((val, version)),
                        Err(err) => Err(Error::InvalidDataFraming(err.to_string())),
                    }
                }) as Box<Stream<Item=(Option<T>, Version), Error=Error> + Send>;

            Ok((handle, stream))
        })
    }

    /// Subscribes for children updates for the node at the specified path.
    ///
    /// This method returns a future, which can be split into a cancellation token and a stream,
    /// which will return the actual list of children on each child creation or deletion. Other
    /// operations, such as children mutation, are not the subject of this method.
    pub fn children_subscribe(&self, path: &str) ->
        impl Future<Item=(Close, Box<Stream<Item=(Version, Vec<String>), Error=Error> + Send>), Error=Error>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        self.service.call(Request::new(Method::ChildrenSubscribe.into(), &[path]).unwrap(), dispatch).and_then(|sender| {
            let handle = Close { sender: sender };
            let stream = box rx.map_err(|()| Error::Canceled)
                .then(Flatten::flatten) as Box<Stream<Item=(Version, Vec<String>), Error=Error> + Send>;
            Ok((handle, stream))
        })
    }
}
