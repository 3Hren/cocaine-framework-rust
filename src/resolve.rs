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

/// Cloud name resolution for services.
///
/// Used before service connection establishing to determine where to connect, i.e where a service
/// with the given name is located.
/// For common usage the most reasonable choice is a [`Resolver`][resolver] implementation that
/// uses [`Locator`][locator] for name resolution.
///
/// [locator]: service/locator/struct.Locator.html
/// [resolver]: struct.Resolver.html
pub trait Resolve {
    /// Future type that is returned during resolving.
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

    /// Returns endpoints given at construction time.
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }
}

/// An implementation of trait for giving a `FixedResolver` a useful default value.
///
/// The default endpoint for a fixed resolver is: `[::1]:10053`.
impl Default for FixedResolver {
    fn default() -> Self {
        FixedResolver {
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
        Box::new(self.locator.resolve(name).map(|info| info.into()))
    }
}

#[cfg(test)]
mod test {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use super::FixedResolver;

    #[test]
    fn fixed_resolver_saves_addrs() {
        let addrs = vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10053),
        ];
        let resolver = FixedResolver::new(addrs.clone());

        assert_eq!(addrs, resolver.addrs());
    }

    #[test]
    fn fixed_resolver_default_addrs() {
        let resolver = FixedResolver::default();

        assert_eq!(vec![SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 10053)],
            resolver.addrs());
    }
}
