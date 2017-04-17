use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use futures::{Future, Stream};
use futures::sync::{mpsc, oneshot};

use {Error, Service};
use dispatch::{PrimitiveDispatch, Streaming, StreamingDispatch};

use flatten_err;

#[derive(Debug, Deserialize)]
pub struct GraphNode {
    event: String,
    rx: HashMap<u64, GraphNode>,
}

#[derive(Debug, Deserialize)]
pub struct EventGraph {
    name: String,
    tx: HashMap<u64, GraphNode>,
    rx: HashMap<u64, GraphNode>,
}

#[derive(Debug, Deserialize)]
pub struct ResolveInfo {
    endpoints: Vec<(IpAddr, u16)>,
    version: u64,
    methods: HashMap<u64, EventGraph>,
}

#[derive(Debug)]
pub struct Info {
    endpoints: Vec<SocketAddr>,
    version: u64,
    methods: HashMap<u64, EventGraph>,
}

impl Info {
    pub fn endpoints(&self) -> &[SocketAddr] {
        &self.endpoints
    }
}

pub type HashRing = Vec<(u64, String)>;

#[derive(Debug)]
pub struct Locator {
    service: Service,
}

impl Locator {
    pub fn new(service: Service) -> Self {
        Locator { service: service }
    }

    pub fn resolve(&self, name: &str) -> impl Future<Item = Info, Error = Error> {
        let (tx, rx) = oneshot::channel();
        let dispatch = PrimitiveDispatch::new(tx);

        self.service.call(0, &[name], dispatch);

        rx.then(flatten_err).map(|ResolveInfo{endpoints, version, methods}| {
            let endpoints = endpoints.into_iter()
                .map(|(ip, port)| SocketAddr::new(ip, port))
                .collect();

            Info {
                endpoints: endpoints,
                version: version,
                methods: methods,
            }
        })
    }

    pub fn routing(&self, uuid: &str) ->
        impl Stream<Item = Streaming<HashMap<String, HashRing>>, Error = ()>
    {
        let (tx, rx) = mpsc::unbounded();
        let dispatch = StreamingDispatch::new(tx);
        self.service.call(5, &[uuid], dispatch);
        rx
    }
}
