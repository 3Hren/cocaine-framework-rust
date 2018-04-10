//! Unified configuration service API.

use futures::{Future, Stream};
use futures::sync::mpsc;

use rmpv::{self, Value};

use serde::{Deserialize, Serialize};

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
    /// Closes this handle, notifying a service side that we're no longer interested in receiving
    /// updates.
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
    Put,
    Get,
    Create,
    Del,
}

impl Into<u64> for Method {
    #[inline]
    fn into(self) -> u64 {
        match self {
            Method::Subscribe => 0,
            Method::ChildrenSubscribe => 1,
            Method::Put => 2,
            Method::Get => 3,
            Method::Create => 4,
            Method::Del => 5,
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

    /// Creates record at specified path with provided value.
    ///
    /// This method returns a optional boolean value of operation result
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
    /// let future = unicorn.create("/cocaine/config", &vec![1,2,3], None);
    ///
    /// let result: bool = core.run(future).unwrap();
    /// ```
    pub fn create<T, H>(&self, path: &str, value: &T, headers: H) ->
        impl Future<Item=bool, Error=Error>
    where
        T: Serialize,
        H: IntoIterator<Item=RawHeader>
    {
        let (dispatch, future) = PrimitiveDispatch::pair();
        let request = Request::new(Method::Create.into(), &(path, value))
            .unwrap()
            .add_headers(headers);

        self.service.call(request, dispatch);
        future.and_then(|val: Value| {
            match rmpv::ext::deserialize_from(val) {
                Ok(val)  => Ok(val),
                Err(err) => Err(Error::InvalidDataFraming(err.to_string())),
            }
        })
    }

    /// Writes record at specified path with provided version.
    ///
    /// This method returns a optional boolean value of operation result,
    /// plus assigned version.
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
    /// let future = unicorn.put("/cocaine/config", &vec![1,2,3], None);
    ///
    /// let result: (bool, i64) = core.run(future).unwrap();
    /// ```
    pub fn put<T, H>(&self, path: &str, value: &T, headers: H) ->
        impl Future<Item=(bool, Version), Error=Error>
    where
        T: Serialize,
        H: IntoIterator<Item=RawHeader>
    {
        let (dispatch, future) = PrimitiveDispatch::pair();
        let request = Request::new(Method::Put.into(), &(path, value))
            .unwrap()
            .add_headers(headers);

        self.service.call(request, dispatch);
        future.and_then(|(val, version): (Value, Version)| {
            match rmpv::ext::deserialize_from(val) {
                Ok(val) => Ok((val, version)),
                Err(err) => Err(Error::InvalidDataFraming(err.to_string())),
            }
        })
    }

    /// Deletes the record at specified path with provided version.
    ///
    /// This method returns a optional boolean value of operation result.
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
    /// let future = unicorn.del("/cocaine/config", &(42 as i64), None);
    ///
    /// let result: bool = core.run(future).unwrap();
    /// ```
    pub fn del<H>(&self, path: &str, version: &Version, headers: H) ->
        impl Future<Item=bool, Error=Error>
    where
        H: IntoIterator<Item=RawHeader>
    {
        let (dispatch, future) = PrimitiveDispatch::pair();
        let request = Request::new(Method::Del.into(), &(path, version))
            .unwrap()
            .add_headers(headers);

        self.service.call(request, dispatch);
        future.and_then(|val: Value| {
            match rmpv::ext::deserialize_from(val) {
                Ok(val) => Ok(val),
                Err(err) => Err(Error::InvalidDataFraming(err.to_string())),
            }
        })
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
    /// let future = unicorn.get("/cocaine/config", None);
    ///
    /// let (value, version): (Option<String>, i64) = core.run(future).unwrap();
    /// ```
    pub fn get<T, H>(&self, path: &str, headers: H) ->
        impl Future<Item=(Option<T>, Version), Error=Error>
    where
        T: for<'de> Deserialize<'de>,
        H: Into<Option<Vec<RawHeader>>>
    {
        let (dispatch, future) = PrimitiveDispatch::pair();
        let headers = headers.into().unwrap_or_default();
        let request = Request::new(Method::Get.into(), &[path])
            .unwrap()
            .add_headers(headers);

        self.service.call(request, dispatch);
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
    pub fn subscribe<'a, T, H>(&self, path: &str, headers: H) ->
        impl Future<Item=(Close, Box<Stream<Item=(Option<T>, Version), Error=Error> + Send + 'a>), Error=Error>
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
            let handle = Close { sender };
            let stream = rx.map_err(|()| Error::Canceled)
                .then(Flatten::flatten)
                .and_then(|(val, version): (Value, Version)| {
                    match rmpv::ext::deserialize_from(val) {
                        Ok(val) => Ok((val, version)),
                        Err(err) => Err(Error::InvalidDataFraming(err.to_string())),
                    }
                });
            let stream = Box::new(stream) as Box<Stream<Item=(Option<T>, Version), Error=Error> + Send>;

            Ok((handle, stream))
        })
    }

    /// Subscribes for children updates for the node at the specified path.
    ///
    /// This method returns a future, which can be split into a cancellation token and a stream,
    /// which will return the actual list of children on each child creation or deletion. Other
    /// operations, such as children mutation, are not the subject of this method.
    pub fn children_subscribe<H>(&self, path: &str, headers: H) ->
        impl Future<Item=(Close, Box<Stream<Item=(Version, Vec<String>), Error=Error> + Send>), Error=Error>
    where
        H: Into<Option<Vec<RawHeader>>>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        let headers = headers.into().unwrap_or_default();
        let request = Request::new(Method::ChildrenSubscribe.into(), &[path])
            .unwrap()
            .add_headers(headers);

        self.service.call(request, dispatch).and_then(|sender| {
            let handle = Close { sender };
            let stream = rx.map_err(|()| Error::Canceled)
                .then(Flatten::flatten);
            let stream = Box::new(stream) as Box<Stream<Item=(Version, Vec<String>), Error=Error> + Send>;
            Ok((handle, stream))
        })
    }
}
