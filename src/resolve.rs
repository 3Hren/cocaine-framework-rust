use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use futures::{future, Future};

use Error;
use service::locator::{EventGraph, Locator};

#[derive(Debug)]
pub struct ResolveInfo {
    addrs: Vec<SocketAddr>,
    methods: Option<HashMap<u64, EventGraph>>,
}

impl ResolveInfo {
    pub fn new(addrs: Vec<SocketAddr>, methods: Option<HashMap<u64, EventGraph>>) -> Self {
        Self {
            addrs: addrs,
            methods: methods,
        }
    }

    pub fn into_components(self) -> (Vec<SocketAddr>, Option<HashMap<u64, EventGraph>>) {
        (self.addrs, self.methods)
    }
}

// TODO: Adapt documentation.
/// Cloud name resolution for services.
///
/// Used before service connection establishing to determine where to connect, i.e where a service
/// with the given name is located.
/// For common usage the most reasonable choice is a [`Resolver`][resolver] implementation that
/// uses [`Locator`][locator] for name resolution.
///
/// [locator]: struct.Locator.html
/// [resolver]: struct.Resolver.html
pub trait Resolve {
    type Future: Future<Item=ResolveInfo, Error=Error>;

    /// Resolves a service name into the network endpoints.
    fn resolve(&mut self, name: &str) -> Self::Future;
}

/// A no-op resolver, that always returns preliminarily specified endpoints.
///
/// Used primarily while resolving a `Locator` itself, but can be also used, when you're sure about
/// service's location.
///
/// The default value returns the default `Locator` endpoints, i.e `["::", 10053]` assuming that
/// IPv6 is enabled.
#[derive(Clone, Debug)]
pub struct FixedResolver {
    addrs: Vec<SocketAddr>,
}

impl FixedResolver {
    /// Constructs a fixed resolver, which will always resolve any service name into the specified
    /// endpoints.
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        FixedResolver {
            addrs: addrs,
        }
    }
}

impl Default for FixedResolver {
    fn default() -> Self {
        FixedResolver {
            // TODO: Replace with dual-stack endpoints. Test.
            addrs: vec![SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 10053)],
        }
    }
}

impl Resolve for FixedResolver {
    type Future = future::FutureResult<ResolveInfo, Error>;

    fn resolve(&mut self, _name: &str) -> Self::Future {
        let result = ResolveInfo {
            addrs: self.addrs.clone(),
            methods: None,
        };

        future::ok(result)
    }
}

/// A `Resolver` that user the `Locator` for name resolution.
#[derive(Debug)]
pub struct Resolver {
    locator: Locator,
}

impl Resolver {
    /// Constructs a new `Resolver` using the specified `Locator` for name resolution.
    pub fn new(locator: Locator) -> Self {
        Self {
            locator: locator,
        }
    }
}

impl Resolve for Resolver {
    type Future = Box<Future<Item=ResolveInfo, Error=Error>>;

    fn resolve(&mut self, name: &str) -> Self::Future {
        box self.locator.resolve(name).map(|info| info.into())
    }
}
