//! Storage service API.

use std::mem;

use futures::Future;

use {Error, Request, Service};
use dispatch::PrimitiveDispatch;

/// Storage service wrapper.
#[derive(Clone, Debug)]
pub struct Storage {
    service: Service,
}

impl Storage {
    /// Constructs a new storage service wrapper using the specified service.
    pub fn new(service: Service) -> Self {
        Self { service }
    }

    /// Consumes this wrapper, yielding an underlying service.
    pub fn into_inner(self) -> Service {
        self.service
    }

    /// Reads the data that is kept at the specified collection and key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cocaine::{Core, Service};
    /// use cocaine::service::Storage;
    ///
    /// let mut core = Core::new().unwrap();
    /// let storage = Storage::new(Service::new("storage", &core.handle()));
    ///
    /// let future = storage.read("collection", "key");
    ///
    /// let data = core.run(future).unwrap();
    /// ```
    pub fn read(&self, collection: &str, key: &str) -> impl Future<Item = Vec<u8>, Error = Error> {
        let (dispatch, future) = PrimitiveDispatch::pair();
        self.service.call(Request::new(0, &(collection, key)).unwrap(), dispatch)
            .map(|sender| {
                mem::drop(sender);
            })
            .and_then(|()| future)
    }

    /// Writes the specified data into the storage.
    ///
    /// Optional indexes can also be associated with the data written.
    ///
    /// Returns a future, which will be resolved either when the data is successfully written into
    /// the storage or some error occurred.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cocaine::{Core, Service};
    /// use cocaine::service::Storage;
    ///
    /// let mut core = Core::new().unwrap();
    /// let storage = Storage::new(Service::new("storage", &core.handle()));
    ///
    /// let future = storage.write("collection", "key", "le message".as_bytes(), &[]);
    ///
    /// core.run(future).unwrap();
    /// ```
    pub fn write(&self, collection: &str, key: &str, data: &[u8], indexes: &[&str]) ->
        impl Future<Item = (), Error = Error>
    {
        // This is required, because of old MessagePack on the server-side.
        let data: &str = unsafe { mem::transmute(data) };

        let (dispatch, future) = PrimitiveDispatch::pair();
        self.service.call(Request::new(1, &(collection, key, data, indexes)).unwrap(), dispatch)
            .map(|sender| {
                mem::drop(sender);
            })
            .and_then(|()| future)
    }
}
