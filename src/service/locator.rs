use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use futures::Future;
use futures::sync::oneshot;

use rmpv::ValueRef;

use {Dispatch, Error, Service};
use protocol::{self, Flatten, Primitive};

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

struct ResolveDispatch {
    tx: oneshot::Sender<Result<Info, Error>>,
}

impl Dispatch for ResolveDispatch {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        let result = protocol::deserialize::<Primitive<ResolveInfo>>(ty, response)
            .flatten()
            .map(|ResolveInfo{endpoints, version, methods}|
        {
            let endpoints = endpoints.into_iter()
                .map(|(ip, port)| SocketAddr::new(ip, port))
                .collect();

            Info {
                endpoints: endpoints,
                version: version,
                methods: methods,
            }
        });

        drop(self.tx.send(result));

        None
    }

    fn discard(self: Box<Self>, err: &Error) {
        drop(self.tx.send(Err(err.clone())));
    }
}

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
        let dispatch = ResolveDispatch { tx: tx };

        self.service.call(0, &[name], dispatch);

        rx.then(|res| match res {
            Ok(Ok(vec)) => Ok(vec),
            Ok(Err(err)) => Err(err),
            Err(oneshot::Canceled) => Err(Error::Canceled),
        })
    }
}
