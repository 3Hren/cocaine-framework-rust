//! Ticket Vending Machine security service.

use std::collections::HashMap;

use futures::Future;

use {Error, Service};
use dispatch::PrimitiveDispatch;

/// A grant type.
#[derive(Clone, Debug)]
pub enum Grant {
    /// This can be exchanged to a ticket with basic client credentials, like client id.
    ClientCredentials,
}

impl Grant {
    fn ty(&self) -> &str {
        match *self {
            Grant::ClientCredentials => "client_credentials",
        }
    }
}

enum Method {
    TicketFull,
}

impl Into<u64> for Method {
    #[inline]
    fn into(self) -> u64 {
        match self {
            Method::TicketFull => 1,
        }
    }
}

/// A service wrapper for the Yandex TVM service.
#[derive(Clone, Debug)]
pub struct Tvm {
    service: Service,
}

impl Tvm {
    /// Constructs a TVM service wrapper using the specified service.
    pub fn new(service: Service) -> Self {
        Self { service: service }
    }

    /// Unwraps this TVM service yielding the underlying service.
    pub fn into_inner(self) -> Service {
        self.service
    }

    /// Exchanges your credentials for a TVM ticket.
    pub fn ticket(&self, id: u32, secret: &str, grant: &Grant) ->
        impl Future<Item = String, Error = Error>
    {
        let method = Method::TicketFull.into();
        let ty = grant.ty();
        let args: HashMap<String, String> = HashMap::new();

        let (dispatch, future) = PrimitiveDispatch::pair();

        match *grant {
            Grant::ClientCredentials => {
                self.service.call(method, &(id, secret, ty, args), Vec::new(), dispatch);
            }
        }

        future
    }
}

#[cfg(test)]
mod test {}
