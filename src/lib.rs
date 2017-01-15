#![feature(box_syntax)]
#![feature(conservative_impl_trait)]

#[macro_use]
extern crate log;
extern crate futures;
extern crate serde;
extern crate rmp;
extern crate rmp_serde;
extern crate rmpv;
extern crate tokio_core;

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Display};
use std::io::{self, Cursor, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::{Async, Future, Poll, Stream};
use futures::future;
use futures::stream::Fuse;
use futures::sync::oneshot::{self, Canceled};
use futures::sync::mpsc;

use serde::Serialize;

use tokio_core::io::Window;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;

use rmp_serde::to_vec;
use rmpv::ValueRef;
use rmpv::decode::read_value_ref;

use Async::*;

mod net;

pub use net::connect;

pub trait Dispatch {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>>;
}

enum Event {
    // TODO: Switch to enum struct.
    Call(oneshot::Sender<Result<Sender, Error>>, u64, Vec<u8>, Box<Dispatch>),
    // TODO: Push {channel: u64 ty: u64, data: Vec<u8> },
}

impl Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Event::Call(_, ty, ..) => write!(f, "Event::Call(ty: {})", ty),
        }
    }
}

struct Reader {
    ring: Vec<u8>,
    rd_offset: usize,
    rx_offset: usize,
}

impl Reader {
    fn new() -> Self {
        Reader {
            ring: vec![0; 64],
            rd_offset: 0,
            rx_offset: 0,
        }
    }

    fn read_from<R: Read>(&mut self, rd: &mut R) -> Result<ValueRef, io::Error> {
        unimplemented!();
    }
}

struct RawMultiplex<T> {
    id: u64,
    sock: T,
    tx: mpsc::UnboundedSender<Event>,
    rx: mpsc::UnboundedReceiver<Event>,

    pending: VecDeque<Window<Vec<u8>>>,
    dispatch: HashMap<u64, Box<Dispatch>>,

    ring: Vec<u8>,
    rd_offset: usize,
    rx_offset: usize,
    rd: Reader,
}

impl<T: Read + Write> RawMultiplex<T> {
    fn add_event(&mut self, event: Event) {
        match event {
            Event::Call(tx, ty, buf, dispatch) => {
                self.id += 1;

                let mut head = Vec::new();
                rmp::encode::write_array_len(&mut head, 3).unwrap();
                rmp::encode::write_uint(&mut head, self.id).unwrap();
                rmp::encode::write_uint(&mut head, ty).unwrap();

                self.pending.push_back(Window::new(head));
                self.pending.push_back(Window::new(buf));

                self.dispatch.insert(self.id, dispatch);

                tx.complete(Ok(Sender::new(self.id, self.tx.clone())));
            }
        }
    }
}

impl<T: Read + Write> Future for RawMultiplex<T> {
    type Item = ();
    type Error = io::Error;

    ///
    /// The task is considered completed when all of the following conditions are met: no more
    /// events will be delivered from the event source, all pending events are sent and there are
    /// no more dispathes left.
    fn poll(&mut self) -> Poll<(), io::Error> {
        // Poll incoming write/control events.
        // TODO: Replace with generic event source.
        loop {
            match self.rx.poll() {
                Ok(Ready(Some(event))) => {
                    self.add_event(event);
                }
                Ok(Ready(None)) | Err(()) => {
                    // No more control events will be received. However we must finish all pending
                    // socket operations.
                    return Ok(Ready(()));
                }
                Ok(NotReady) => {
                    break;
                }
            }
        }

        // Flush unwritten.
        // TODO: Add scatter/gather support sometimes.
        while let Some(mut buf) = self.pending.pop_front() {
            match self.sock.write(buf.as_ref()) {
                Ok(nsize) if nsize == buf.as_ref().len() => {
                    debug!("written {} bytes", nsize);
                }
                Ok(nsize) => {
                    debug!("written {} bytes", nsize);
                    let from = buf.start();
                    buf.set_start(from + nsize);
                    self.pending.push_front(buf);
                }
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    self.pending.push_front(buf);
                    break;
                }
                Err(..) => {
                    // TODO: Probably we shouldn't return here even if writer part was shutted down
                    // to be able to read entirely.
                    unimplemented!();
                }
            }
        }

        // Read.
        loop {
            // TODO: Read only if dispatch.len() > 0.
            // match self.rd.read_from(&mut self.sock) {
            //     Ok(data) => {}
            //     Err(err) => {
            //         // Either I/O or framing error.
            //     }
            // }

            match self.sock.read(&mut self.ring[self.rd_offset..]) {
                Ok(0) => {
                    debug!("EOF");
                    break;
                }
                Ok(nread) => {
                    self.rd_offset += nread;
                    debug!("read {} bytes; Ring {{ rx: {}, rd: {}, len: {} }}",
                        nread, self.rx_offset, self.rd_offset, self.ring.len());
                    { // TODO: Stupid lifetimes!
                        let mut rdbuf = Cursor::new(&self.ring[self.rx_offset..self.rd_offset]);
                        match read_value_ref(&mut rdbuf) {
                            Ok(val) => {
                                debug!("-> {}", val);
                                self.rx_offset += rdbuf.position() as usize;

                                let id = val.index(0).as_u64().unwrap();
                                let ty = val.index(1).as_u64().unwrap();
                                let args = &val.index(2);

                                match self.dispatch.remove(&id) {
                                    Some(dispatch) => {
                                        match dispatch.process(ty, args) {
                                            Some(dispatch) => {
                                                self.dispatch.insert(id, dispatch);
                                            }
                                            None => {
                                                debug!("revoked channel {}", id);
                                            }
                                        }
                                    }
                                    None => {
                                        warn!("dropped unexpected value");
                                    }
                                }
                            }
                            Err(ref err) if err.insufficient_bytes() => {
                                debug!("failed to decode frame - insufficient bytes");
                            }
                            Err(err) => {
                                error!("failed to decode value from read buffer {:?}", err);
                                // TODO: Framing error. We should terminate the connection.
                                unimplemented!();
                            }
                        }
                    }

                    let pending = self.rd_offset - self.rx_offset;
                    if self.rx_offset != 0 {
                        // TODO: Consider less often ring compactifying.
                        self.ring.drain(0..self.rd_offset);

                        self.rd_offset = pending;
                        self.rx_offset = 0;
                        debug!("compactified the ring");
                    }

                    let len = self.ring.len();
                    if pending * 2 >= len {
                        // The total size of unprocessed data in larger than half the size
                        // of the ring, so grow the ring in order to accomodate more data.
                        self.ring.resize(len * 2, 0);
                        debug!("resized rdbuf to {}", self.ring.len());
                    }
                }
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(..) => {
                    // TODO: Probably we shouldn't return here even if reader part was shut down
                    // to be able to write entirely.
                    unimplemented!();
                }
            }
        }

        Ok(NotReady)
    }
}

pub struct Sender {
    id: u64,
    tx: mpsc::UnboundedSender<Event>,
}

impl Sender {
    fn new(id: u64, tx: mpsc::UnboundedSender<Event>) -> Self {
        Sender { id: id, tx: tx }
    }
}

#[derive(Clone)]
pub struct Service_ {
    peer: SocketAddr,
    tx: Arc<Mutex<mpsc::UnboundedSender<Event>>>,
}

impl Service_ {
    pub fn connect<E>(endpoints: E, handle: &Handle) -> impl Future<Item = Service_, Error = io::Error>
        where E: IntoIterator<Item = SocketAddr>
    {
        let h = handle.clone();
        connect(endpoints, handle).and_then(move |sock| {
            let peer = match sock.peer_addr() {
                Ok(peer) => peer,
                Err(err) => return future::done(Err(err)),
            };

            info!("successfully connected to {}", peer);
            let (tx, rx) = mpsc::unbounded();

            let service = Service_ {
                peer: peer,
                tx: Arc::new(Mutex::new(tx.clone())),
            };

            let mx = RawMultiplex {
                id: 0,
                sock: sock,
                tx: tx,
                rx: rx,
                pending: VecDeque::new(),
                dispatch: HashMap::new(),

                ring: vec![0; 64],
                rd_offset: 0,
                rx_offset: 0,
                rd: Reader::new(),
            };

            h.spawn(mx.map_err(|_| ()));

            future::done(Ok(service))
        })
    }

    pub fn peer_addr(&self) -> &SocketAddr {
        &self.peer
    }

    pub fn call<T, D>(&self, ty: u64, args: &T, dispatch: D) -> impl Future<Item=Sender, Error=Error>
        where T: Serialize,
              D: Dispatch + 'static
    {
        let (tx, rx) = oneshot::channel();

        let buf = to_vec(args).unwrap();

        // TODO: May be bottleneck.
        self.tx.lock().unwrap().send(Event::Call(tx, ty, buf, box dispatch)).unwrap();

        rx.then(|send| {
            match send {
                Ok(Ok(send)) => Ok(send),
                Ok(Err(err)) => Err(err),
                Err(Canceled) => Err(Error::Canceled),
            }
        })
    }
}

#[derive(Debug)]
pub enum Error {
    /// Operation has been aborted due to I/O error.
    Io(io::Error),
    /// Service error with type, category and optional description.
    Service(u64, u64, Option<String>),
    /// Operation has been canceled internally due to unstoppable forces.
    Canceled,
}

fn resolve(name: Cow<'static, str>, handle: &Handle) ->
    impl Future<Item = Vec<SocketAddr>, Error = Error>
{
    use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10053);
    Service_::connect(vec![addr], handle)
        .map_err(Error::Io)
        .and_then(move |service| {
            let (tx, rx) = oneshot::channel();

            struct ResolveDispatch {
                tx: oneshot::Sender<Vec<SocketAddr>>,
            };

            impl Dispatch for ResolveDispatch {
                fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
                    // TODO: Will be eliminated using enum match.
                    match ty {
                        0 => {
                            // TODO: Will be eliminated using serde and high-level bindings.
                            let mut vec = Vec::new();
                            for addr in response.index(0).to_owned().as_array().unwrap() {
                                vec.extend((addr[0].as_str().unwrap(), addr[1].as_u64().unwrap() as u16).to_socket_addrs().unwrap());
                            }

                            self.tx.complete(vec);
                        }
                        _ => {
                            unimplemented!();
                        }
                    }

                    None
                }
            }

            let dispatch = ResolveDispatch {
                tx: tx,
            };

            service.call(0, &vec![name], dispatch);

            rx.map_err(|_| Error::Canceled)
        })
}

enum State {
    Disconnected,
    Resolving(Box<Future<Item=Vec<SocketAddr>, Error=Error>>),
    Connecting(Box<Future<Item=TcpStream, Error=io::Error>>),
    Running(RawMultiplex<TcpStream>),
}

impl Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            State::Disconnected => write!(fmt, "State::Disconnected"),
            State::Resolving(..) => write!(fmt, "State::Resolving"),
            State::Connecting(..) => write!(fmt, "State::Connecting"),
            State::Running(..) => write!(fmt, "State::Running"),
        }
    }
}

impl Display for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            State::Disconnected => write!(fmt, "disconnected"),
            State::Resolving(..) => write!(fmt, "resolving"),
            State::Connecting(..) => write!(fmt, "connecting"),
            State::Running(..) => write!(fmt, "running"),
        }
    }
}

struct Multiplex {
    // Service name for resolving.
    name: Cow<'static, str>,
    // State.
    state: Option<State>,
    // Event channel.
    rx: Fuse<mpsc::UnboundedReceiver<Event>>,
    // Event loop notifier.
    handle: Handle,
    // Saved events while not being connected.
    events: VecDeque<Event>,
}

impl Multiplex {
}

impl Future for Multiplex {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        debug!("poll multiplex, state: {}", self.state.as_ref().unwrap());

        match self.state.take().expect("failed to extract internal state") {
            State::Disconnected => {
                // The only reason we've been woken up here - new events. There can be both
                // invocation and cancellation events.
                match self.rx.poll() {
                    Ok(Ready(Some(event))) => {
                        self.events.push_back(event);
                        debug!("pushed event into the queue, pending: {}", self.events.len());

                        self.state = Some(State::Resolving(box resolve(self.name.clone(), &self.handle)));
                        debug!("switched state from `disconnected` to `resolving`");
                        return self.poll();
                    }
                    Ok(Ready(None)) | Err(()) => {
                        // Channel between user and us has been closed and we're disconnected.
                        info!("service state machine has been terminated");
                        return Ok(Ready(()));
                    }
                    Ok(NotReady) => {
                        unreachable!();
                    }
                }
            }
            State::Resolving(mut future) => {
                loop {
                    match self.rx.poll() {
                        Ok(Ready(Some(event))) => {
                            self.events.push_back(event);
                            debug!("pushed event into the queue, pending: {}", self.events.len());
                        }
                        Ok(..) | Err(()) => {
                            // No new events will be delivered, because there are no more senders,
                            // however we're ok with fire-and-forget strategy, so move forward.
                            break;
                        }
                    }
                }

                match future.poll() {
                    Ok(Ready(addrs)) => {
                        info!("successfully resolved `{}` service", self.name);
                        self.state = Some(State::Connecting(box connect(addrs, &self.handle)));
                        return self.poll();
                    }
                    Ok(NotReady) => {
                        self.state = Some(State::Resolving(future));
                    }
                    Err(err) => {
                        for event in self.events.drain(..) {}
                    }
                }
            }
            State::Connecting(mut future) => {
                // TODO: Extract pending control events.
                match future.poll() {
                    Ok(Ready(sock)) => {
                        let peer = sock.peer_addr()?;

                        info!("successfully connected to {}", peer);

                        let (tx, rx) = mpsc::unbounded();

                        let mut mx = RawMultiplex {
                            id: 0,
                            sock: sock,
                            tx: tx,
                            rx: rx,
                            pending: VecDeque::new(),
                            dispatch: HashMap::new(),

                            ring: vec![0; 64],
                            rd_offset: 0,
                            rx_offset: 0,
                            rd: Reader::new(),
                        };

                        for event in self.events.drain(..) {
                            mx.add_event(event);
                        }

                        self.state = Some(State::Running(mx));
                        return self.poll();
                    }
                    Ok(NotReady) => {
                        self.state = Some(State::Connecting(future));
                        debug!("connecting - not ready");
                    }
                    Err(..) => {
                        debug!("connecting - canceled");
                        // TODO: Make disconnected state or what?
                    }
                }
            }
            State::Running(mut future) => {
                // TODO: New events, socket events. Should be moved to `handle` when it's time to
                // be disconnected to be able to handle pending send/recv events.
                match future.poll() {
                    Ok(Ready(())) => {
                        self.state = Some(State::Disconnected);
                    }
                    Ok(NotReady) => {
                        self.state = Some(State::Running(future));
                        debug!("running - not ready");
                    }
                    Err(..) => {
                        self.state = Some(State::Disconnected);
                    }
                }
            }
        }

        Ok(NotReady)
    }
}

/// Same as service, but provides name resolution and reconnecting before the next request after
/// I/O error.
#[derive(Clone)]
pub struct Service {
    name: Cow<'static, str>,
    tx: Arc<Mutex<mpsc::UnboundedSender<Event>>>,
}

impl Service {
    ///
    /// # Note
    ///
    /// Name resolution and connection will be established on demand.
    pub fn new<N>(name: N, handle: &Handle) -> Self
        where N: Into<Cow<'static, str>>
    {
        let name = name.into();

        let (tx, rx) = mpsc::unbounded();

        let mx = Multiplex {
            name: name.clone(),
            state: Some(State::Disconnected),
            rx: rx.fuse(),
            handle: handle.clone(),
            events: VecDeque::new(),
        };

        handle.spawn(mx.map_err(|err| warn!("stopped multiplex task: {:?}", err) ));

        Service {
            name: name,
            tx: Arc::new(Mutex::new(tx.clone())),
        }
    }

    /// Returns service name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn call<T, D>(&self, ty: u64, args: &T, dispatch: D) -> impl Future<Item = Sender, Error = Error>
        where T: Serialize,
              D: Dispatch + 'static
    {
        let (tx, rx) = oneshot::channel();

        let buf = to_vec(args).unwrap();

        // TODO: May be bottleneck.
        self.tx.lock().unwrap().send(Event::Call(tx, ty, buf, box dispatch)).unwrap();

        rx.then(|send| {
            match send {
                Ok(Ok(send)) => Ok(send),
                Ok(Err(err)) => Err(err),
                Err(Canceled) => Err(Error::Canceled),
            }
        })
    }
}
