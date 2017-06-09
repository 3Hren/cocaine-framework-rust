//! This module contains various simplifications aimed to ease working with services: builders and
//! façades.

use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::iter::IntoIterator;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio_core::reactor::Handle;

use {Resolve, Service};
use super::{FixedResolver, Resolver, Supervisor};

pub mod app;
pub mod locator;
pub mod unicorn;

pub use self::app::App;
pub use self::locator::Locator;
pub use self::unicorn::Unicorn;

const LOCATOR_NAME: &str = "locator";

/// Resolve configuration.
pub trait ResolveBuilder {
    type Item: Resolve;

    fn build(self, handle: &Handle) -> Self::Item;
}

#[derive(Debug)]
struct ResolverBuilder {
    name: Cow<'static, str>,
    resolver: FixedResolver,
}

impl ResolveBuilder for ResolverBuilder {
    type Item = Resolver;

    fn build(self, handle: &Handle) -> Self::Item {
        let shared = Arc::new(Mutex::new(Default::default()));

        let locator = Service {
            name: self.name.clone(),
            shared: shared.clone(),
            tx: Supervisor::spawn(self.name, shared, self.resolver, handle),
        };

        Resolver::new(Locator::new(locator))
    }
}

/// A resolver builder that returns already preconfigured `Resolve`.
#[derive(Debug)]
pub struct PreparedResolver<R> {
    resolver: R,
}

impl<R: Resolve> ResolveBuilder for PreparedResolver<R> {
    type Item = R;

    fn build(self, _handle: &Handle) -> Self::Item {
        self.resolver
    }
}

/// Service configuration. Provides detailed control over the properties and behavior of new
/// services.
///
/// Uses `Locator` as a service name resolver by default.
///
/// # Examples
///
/// ```
/// use cocaine::{Core, ServiceBuilder};
///
/// let core = Core::new().unwrap();
///
/// let service = ServiceBuilder::new("storage")
///     .build(&core.handle());
///
/// assert_eq!("storage", service.name());
/// ```
pub struct ServiceBuilder<T> {
    name: Cow<'static, str>,
    resolve_builder: T,
}

impl ServiceBuilder<ResolverBuilder> {
    /// Constructs a new service builder, which will build a `Service` with the given name.
    pub fn new<N: Into<Cow<'static, str>>>(name: N) -> Self {
        let resolver = ResolverBuilder {
            name: LOCATOR_NAME.into(),
            resolver: FixedResolver::default(),
        };

        Self {
            name: name.into(),
            resolve_builder: resolver,
        }
    }

    /// Sets the `Locator` resolver endpoints.
    ///
    /// By default `[::]:10053` address is used to resolve the `Locator` service and it can be
    /// changed using this method. If multiple endpoints are specified the resolver will try to
    /// connect to each of them in a loop, breaking after the connection establishment.
    ///
    /// # Note
    ///
    /// These endpoints are used only for `Locator` resolving. The `Service` resolution is done
    /// through the `Locator` unless `FixedResolver` specified using [`resolver`][resolver] method.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    ///
    /// use cocaine::{Core, ServiceBuilder};
    ///
    /// let core = Core::new().unwrap();
    ///
    /// let service = ServiceBuilder::new("storage")
    ///     .locator_addrs(vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10071)])
    ///     .build(&core.handle());
    /// ```
    ///
    /// [resolver]: #method.resolver
    pub fn locator_addrs<E>(mut self, addrs: E) -> Self
        where E: IntoIterator<Item = SocketAddr>
    {
        self.resolve_builder.resolver = FixedResolver::new(addrs.into_iter().collect());
        self
    }

    /// Sets the memory limit in bytes for internal buffers.
    ///
    /// Normally cocaine-runtime must read all incoming events as fast as possible no matter what.
    /// However, especially for logging service, sometimes the client can overflow the TCP window,
    /// which leads to readable stream overload. As a result - we start to buffering incoming
    /// events more and more, and it may potentially lead to OOM killer coming.
    ///
    /// By specifying this option we set the internal memory limit so every new either invocation
    /// or push event, that will overflow the specified limit, will be rejected
    /// with `MemoryOverflow` error code and this is guaranteed that those bytes won't be written
    /// into the socket until retried.
    pub fn memory_limit(self, _nbytes: usize) -> Self {
        // TODO: To allow this we must return a future from `Sender::send`.
        unimplemented!();
    }

    // TODO: Receiver memory_limit.
    // TODO: Resolve timeout.
}

impl<T> ServiceBuilder<T> {
    /// Sets the resolver, that is used for name resolution.
    ///
    /// By default name resolution via the `Locator` is used, but sometimes more detailed control
    /// is required.
    ///
    /// # Examples
    ///
    /// This example demonstrates how to build a `Service`, which will always try to connect to a
    /// fixed endpoint at `127.0.0.1:32768`.
    ///
    /// ```
    /// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    ///
    /// use cocaine::{ServiceBuilder, Core, FixedResolver};
    ///
    /// let core = Core::new().unwrap();
    /// let endpoints = vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 32768)];
    ///
    /// let service = ServiceBuilder::new("storage")
    ///     .resolver(FixedResolver::new(endpoints))
    ///     .build(&core.handle());
    /// ```
    pub fn resolver<R: Resolve>(self, resolver: R) -> ServiceBuilder<PreparedResolver<R>> {
        ServiceBuilder {
            name: self.name,
            resolve_builder: PreparedResolver { resolver: resolver },
        }
    }
}

impl<T: ResolveBuilder + 'static> ServiceBuilder<T> {
    /// Consumes this `ServiceBuilder` yielding a `Service`.
    ///
    /// This will not perform a connection attempt until required - both name resolution and
    /// connection will be performed on demand. You can call [`Service::connect()`][connect] method
    /// for fine-grained control.
    ///
    /// [connect]: ./struct.Service.html#method.connect
    pub fn build(self, handle: &Handle) -> Service {
        let shared = Arc::new(Mutex::new(Default::default()));

        Service {
            name: self.name.clone(),
            shared: shared.clone(),
            tx: Supervisor::spawn(self.name, shared, self.resolve_builder.build(handle), handle),
        }
    }
}

impl<T> Debug for ServiceBuilder<T> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("ServiceBuilder").field("name", &self.name).finish()
    }
}
