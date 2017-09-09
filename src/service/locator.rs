//! Locator service API.

use std::collections::HashMap;
use std::net::SocketAddr;

use futures::{Future, Stream};
use futures::sync::mpsc;

use {Error, Request, Service};
use dispatch::{PrimitiveDispatch, StreamingDispatch};
use protocol::Flatten;
use resolve::ResolveInfo as OuterResolveInfo;

/// Describes a protocol graph node.
#[derive(Clone, Debug, Deserialize)]
pub struct GraphNode {
    /// Event name.
    pub event: String,
    /// Optional downstream protocol description.
    pub rx: Option<HashMap<u64, GraphNode>>,
}

/// Describes a protocol graph for an event.
#[derive(Clone, Debug, Deserialize)]
pub struct EventGraph {
    /// Event name.
    pub name: String,
    /// Optional upstream protocol description.
    pub tx: HashMap<u64, GraphNode>,
    /// Optional downstream protocol description.
    pub rx: HashMap<u64, GraphNode>,
}

/// Resolve response.
#[derive(Debug, Deserialize)]
pub struct ResolveInfo<T> {
    addrs: Vec<T>,
    version: u64,
    methods: HashMap<u64, EventGraph>,
}

#[derive(Clone, Debug)]
pub struct Info {
    addrs: Vec<SocketAddr>,
    version: u64,
    methods: HashMap<u64, EventGraph>,
}

impl Info {
    /// Returns a view of socket addresses for this resolve info.
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }
}

impl Into<OuterResolveInfo> for Info {
    fn into(self) -> OuterResolveInfo {
        OuterResolveInfo::new(self.addrs, Some(self.methods))
    }
}

/// Represents a consistent hash ring where routing groups live.
pub type HashRing = Vec<(u64, String)>;

enum Method {
    Resolve,
    Routing,
}

impl Into<u64> for Method {
    #[inline]
    fn into(self) -> u64 {
        match self {
            Method::Resolve => 0,
            Method::Routing => 5,
        }
    }
}

/// Locator service wrapper.
#[derive(Clone, Debug)]
pub struct Locator {
    service: Service,
}

impl Locator {
    /// Constructs a new Locator service wrapper using the specified service object.
    pub fn new(service: Service) -> Self {
        Self { service }
    }

    /// Resolves a service with the specified name, returning its endpoints, version and methods.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cocaine::{Core, Service};
    /// use cocaine::service::Locator;
    ///
    /// let mut core = Core::new().unwrap();
    /// let locator = Locator::new(Service::new("locator", &core.handle()));
    ///
    /// let future = locator.resolve("node");
    ///
    /// let info = core.run(future).unwrap();
    /// ```
    pub fn resolve(&self, name: &str) -> impl Future<Item = Info, Error = Error> {
        let (dispatch, future) = PrimitiveDispatch::pair();
        self.service.call(Request::new(Method::Resolve.into(), &[name]).unwrap(), dispatch);

        future.map(|ResolveInfo{addrs, version, methods}| {
            let addrs = addrs.into_iter()
                .map(|(ip, port)| SocketAddr::new(ip, port))
                .collect();

            Info {
                addrs: addrs,
                version: version,
                methods: methods,
            }
        })
    }

    /// Subscribes for routing groups changes using an unique identifier.
    pub fn routing(&self, uuid: &str) ->
        impl Stream<Item = HashMap<String, HashRing>, Error = Error>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        self.service.call(Request::new(Method::Routing.into(), &[uuid]).unwrap(), dispatch);
        rx.map_err(|()| Error::Canceled).then(Flatten::flatten)
    }
}
