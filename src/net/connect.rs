use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

use futures::{Future, Poll};

use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_core::reactor::Handle;

fn econnrefused() -> Error {
    Error::new(ErrorKind::ConnectionRefused, "connection refused")
}

struct TcpSteamMultiConnect<I> {
    handle: Handle,
    current: Option<TcpStreamNew>,
    endpoints: I,
}

impl<I: Iterator<Item = SocketAddr>> Future for TcpSteamMultiConnect<I> {
    type Item = TcpStream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.current {
            Some(ref mut future) => {
                if let Ok(poll) = future.poll() {
                    return Ok(poll);
                }
            }
            None => {
                match self.endpoints.next() {
                    Some(addr) => {
                        self.current = Some(TcpStream::connect(&addr, &self.handle));
                        return self.poll();
                    }
                    None => return Err(econnrefused()),
                }
            }
        }

        self.current = None;
        self.poll()
    }
}

/// Establishes a TCP socket connection by trying each endpoint in a sequence.
pub fn connect<E>(endpoints: E, handle: &Handle) -> impl Future<Item = TcpStream, Error = Error>
    where E: IntoIterator<Item = SocketAddr>
{
    TcpSteamMultiConnect {
        handle: handle.clone(),
        current: None,
        endpoints: endpoints.into_iter(),
    }
}
