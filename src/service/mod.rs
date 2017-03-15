use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::iter::IntoIterator;
use std::net::SocketAddr;

use tokio_core::reactor::Handle;

use {Resolve, Service};
use super::{FixedResolver, Resolver, Supervisor};

mod locator;

pub use self::locator::Locator;

const LOCATOR_NAME: &'static str = "locator";

pub struct Builder<T> {
    name: Cow<'static, str>,
    handle: Handle,
    resolver: T,
}

impl Builder<Resolver> {
    /// Constructs a new service builder.
    pub fn new<N: Into<Cow<'static, str>>>(name: N, handle: Handle) -> Self {
        let locator = Service {
            name: LOCATOR_NAME.into(),
            tx: Supervisor::spawn(LOCATOR_NAME.into(), FixedResolver::default(), &handle),
        };

        Builder {
            name: name.into(),
            handle: handle,
            resolver: Resolver::new(locator),
        }
    }

    pub fn locator_addrs<E>(mut self, addrs: E) -> Self
        where E: IntoIterator<Item = SocketAddr>
    {
        let resolve = FixedResolver::new(addrs.into_iter().collect());
        let locator = Service {
            name: LOCATOR_NAME.into(),
            tx: Supervisor::spawn(LOCATOR_NAME.into(), resolve, &self.handle),
        };

        self.resolver = Resolver::new(locator);
        self
    }
}

impl<T> Builder<T> {
    pub fn resolver<R: Resolve>(self, resolver: R) -> Builder<R> {
        Builder {
            name: self.name,
            handle: self.handle,
            resolver: resolver,
        }
    }
}

impl<T: Resolve + 'static> Builder<T> {
    pub fn build(self) -> Service {
        Service {
            name: self.name.clone(),
            tx: Supervisor::spawn(self.name, self.resolver, &self.handle),
        }
    }
}

impl<T> Debug for Builder<T> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Builder").field("name", &self.name).finish()
    }
}
