use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use futures::{Future, Stream};
use futures::sync::{mpsc, oneshot};

use {Error, Service};
use dispatch::{PrimitiveDispatch, StreamingDispatch};
use protocol::Flatten;
use resolve::ResolveInfo as OuterResolveInfo;

use flatten_err;

#[derive(Clone, Debug, Deserialize)]
pub struct GraphNode {
    pub event: String,
    pub rx: Option<HashMap<u64, GraphNode>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EventGraph {
    pub name: String,
    pub tx: HashMap<u64, GraphNode>,
    pub rx: HashMap<u64, GraphNode>,
}

#[derive(Debug, Deserialize)]
pub struct ResolveInfo {
    addrs: Vec<(IpAddr, u16)>,
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
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }
}

// TODO: Consider less hacky solution.
impl Into<OuterResolveInfo> for Info {
    fn into(self) -> OuterResolveInfo {
        OuterResolveInfo::new(self.addrs, Some(self.methods))
    }
}

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

#[derive(Clone, Debug)]
pub struct Locator {
    service: Service,
}

impl Locator {
    pub fn new(service: Service) -> Self {
        Self { service: service }
    }

    pub fn resolve(&self, name: &str) -> impl Future<Item = Info, Error = Error> {
        let (tx, rx) = oneshot::channel();
        let dispatch = PrimitiveDispatch::new(tx);

        self.service.call(Method::Resolve.into(), &[name], Vec::new(), dispatch);

        rx.then(flatten_err).map(|ResolveInfo{addrs, version, methods}| {
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

    pub fn routing(&self, uuid: &str) ->
        impl Stream<Item = HashMap<String, HashRing>, Error = Error>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        self.service.call(Method::Routing.into(), &[uuid], Vec::new(), dispatch);
        rx.map_err(|()| Error::Canceled).then(Flatten::flatten)
    }
}
