#![feature(box_syntax)]
#![feature(conservative_impl_trait)]

#[macro_use] extern crate bitflags;
#[macro_use] extern crate log;
extern crate futures;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate rmpv;
extern crate tokio_core;
extern crate nix;
extern crate libc;

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{self, Cursor, ErrorKind, Read, Write};
use std::mem;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::os::unix::io::AsRawFd;
use std::ptr;

use futures::{Async, Future, Poll, Stream};
use futures::future;
use futures::stream::Fuse;
use futures::sync::oneshot::{self, Canceled};
use futures::sync::mpsc;

use serde::Serialize;

use tokio_core::io::Window;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;

use rmpv::ValueRef;
use rmpv::decode::read_value_ref;

use Async::*;

mod frame;
mod net;
mod service;
mod sys;

use net::connect;
use self::frame::Frame;
pub use self::service::{App, Builder, Locator};

pub trait Dispatch {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>>;

    fn discard(self: Box<Self>, err: &Error) {
        let _ = err;
    }
}

enum Event {
    // TODO: Connect/Disconnect.
    Call {
        ty: u64,
        data: Vec<u8>,
        dispatch: Box<Dispatch + Send>,
        tx: oneshot::Sender<Result<u64, Error>>,
    },
    Push {
        id: u64,
        ty: u64,
        data: Vec<u8>,
    },
}

impl Debug for Event {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Event::Call { ty, ref data, .. } => {
                fmt.debug_struct("Event::Call")
                    .field("ty", &ty)
                    .field("len", &data.len())
                    .finish()
            }
            Event::Push { id, ty, ref data } => {
                fmt.debug_struct("Event::Push")
                    .field("id", &id)
                    .field("ty", &ty)
                    .field("len", &data.len())
                    .finish()
            }
        }
    }
}

struct MessageBuf {
    head: Window<[u8; 32]>,
    data: Window<Vec<u8>>,
}

impl MessageBuf {
    fn new(id: u64, ty: u64, data: Vec<u8>) -> Result<Self, io::Error> {
        let mut head = [0; 32];
        let nlen = MessageBuf::encode_head(&mut head[..], id, ty)?;
        let mut head = Window::new(head);
        head.set_end(nlen);

        let mbuf = MessageBuf {
            head: head,
            data: Window::new(data),
        };

        Ok(mbuf)
    }

    fn encode_head(head: &mut [u8], id: u64, ty: u64) -> Result<usize, io::Error> {
        let mut cur = Cursor::new(&mut head[..]);
        // TODO: Support HPACK here.
        rmp::encode::write_array_len(&mut cur, 3)?;
        rmp::encode::write_uint(&mut cur, id)?;
        rmp::encode::write_uint(&mut cur, ty)?;

        Ok(cur.position() as usize)
    }

    fn ulen(&self) -> usize {
        self.head.as_ref().len() + self.data.as_ref().len()
    }

    /// # Panics
    ///
    /// This method will panic if `n` is out of bounds for the underlying slice or if it comes
    /// after the end configured in this message.
    fn eat(&mut self, mut num: usize) {
        if num < self.head.as_ref().len() {
            let from = self.head.start();
            self.head.set_start(from + num);
        } else {
            // Consume head entirely.
            num -= self.head.as_ref().len();
            self.head.set_start(0);
            self.head.set_end(0);

            // Maybe partially consume data.
            let from = self.data.start();
            self.data.set_start(from + num);
        }
    }
}

enum Notify {
    Call(u64, oneshot::Sender<Result<u64, Error>>),
    Push,
}

impl Notify {
    fn complete(self, val: Result<(), io::Error>) {
        match self {
            Notify::Call(id, tx) => drop(tx.send(val.and(Ok(id)).map_err(Error::Io))),
            Notify::Push => {}
        }
    }
}

struct Message {
    mbuf: MessageBuf,
    notify: Notify,
}

impl Message {
    /// Unwritten length.
    fn ulen(&self) -> usize {
        self.mbuf.ulen()
    }

    fn eat(&mut self, n: usize) {
        self.mbuf.eat(n)
    }

    fn complete(self, val: Result<(), io::Error>) {
        self.notify.complete(val)
    }
}


bitflags! {
    flags Shutdown: u8 {
        const CLOSE_SEND = 0b0001,
        const CLOSE_RECV = 0b0010,
        const CLOSE_USER = 0b0100,
    }
}

#[derive(Debug)]
enum MultiplexError {
    /// Operation has been aborted due to I/O error.
    Io(io::Error),
    /// Transport protocol error.
    InvalidProtocol(io::Error),
    /// Framing error.
    InvalidFraming(frame::Error),
}

impl MultiplexError {
    fn clone(&self) -> Self {
        match *self {
            MultiplexError::Io(ref err) => {
                MultiplexError::Io(io::Error::new(err.kind(), error::Error::description(err)))
            }
            MultiplexError::InvalidProtocol(ref err) => {
                MultiplexError::InvalidProtocol(io::Error::new(err.kind(), error::Error::description(err)))
            }
            MultiplexError::InvalidFraming(ref err) => {
                MultiplexError::InvalidFraming(err.clone())
            }
        }
    }
}

impl From<io::Error> for MultiplexError {
    fn from(err: io::Error) -> Self {
        MultiplexError::Io(err)
    }
}

impl<'a> From<rmpv::decode::Error> for MultiplexError {
    fn from(err: rmpv::decode::Error) -> Self {
        MultiplexError::InvalidProtocol(err.into())
    }
}

impl From<frame::Error> for MultiplexError {
    fn from(err: frame::Error) -> Self {
        MultiplexError::InvalidFraming(err)
    }
}

/// Connection multiplexer.
///
/// The task is considered completed when all of the following conditions are met: no more
/// events will be delivered from the event source, all pending events are sent and there are
/// no more dispathes left.
///
/// To match the `Future` contract this future resolves exactly once.
#[must_use = "futures do nothing unless polled"]
struct Multiplex<T> {
    // Request id counter.
    id: u64,
    sock: T,
    peer: SocketAddr,

    // Shutdown state.
    state: Shutdown,

    pending: VecDeque<Message>,
    dispatches: HashMap<u64, Box<Dispatch>>,

    ring: Vec<u8>,
    rd_offset: usize,
    rx_offset: usize,
}

impl<T> Drop for Multiplex<T> {
    fn drop(&mut self) {
        info!("dropped multiplex with connection to {}", self.peer);
    }
}

const IOVEC_MAX: usize = 64;

fn unexpected_eof() -> io::Error {
    io::Error::new(ErrorKind::UnexpectedEof, "unexpected EOF")
}

impl<T: Read + Write + AsRawFd> Multiplex<T> {
    pub fn new(sock: T, peer: SocketAddr) -> Self {
        Multiplex {
            id: 0,
            sock: sock,
            peer: peer,
            state: Shutdown::empty(),
            pending: VecDeque::new(),
            dispatches: HashMap::new(),

            ring: vec![0; 4096],
            rd_offset: 0,
            rx_offset: 0,
        }
    }

    fn add_event(&mut self, event: Event) {
        match event {
            Event::Call { ty, data, dispatch, tx } => {
                self.id += 1;

                let mbuf = MessageBuf::new(self.id, ty, data).unwrap();
                let notify = Notify::Call(self.id, tx);
                let message = Message {
                    mbuf: mbuf,
                    notify: notify,
                };

                self.pending.push_back(message);

                self.dispatches.insert(self.id, dispatch);
            }
            Event::Push { id, ty, data } => {
                let mbuf = MessageBuf::new(id, ty, data).unwrap();
                let message = Message {
                    mbuf: mbuf,
                    notify: Notify::Push,
                };

                self.pending.push_back(message);
            }
        }
    }

    fn send_all(&mut self) -> Result<usize, io::Error> {
        let null = unsafe { mem::uninitialized() };
        let mut size = 0;
        let mut bufs = [null; IOVEC_MAX];
        for (idx, message) in self.pending.iter().enumerate().take(IOVEC_MAX / 2) {
            size += 2;
            bufs[idx * 2] = &message.mbuf.head.as_ref()[..];
            bufs[idx * 2 + 1] = &message.mbuf.data.as_ref()[..];
        }

        // NOTE: Probably `sendmmsg` fits better, but it's linux > 3 only.
        sys::sendmsg(self.sock.as_raw_fd(), &bufs[..size])
    }

    fn poll_send(&mut self) -> Poll<(), MultiplexError> {
        if self.pending.is_empty() && self.state.contains(CLOSE_RECV) {
            // We're detached and live only until there are unflushed messages. Here is the right
            // moment to finish the future.
            return Ok(Ready(()));
        }

        loop {
            // NOTE: For some unknown reasons `sendmsg` raises EMSGSIZE (40) error while trying to
            // send zero buffers.
            if self.pending.is_empty() {
                break;
            }

            debug!("sending {} pending buffer(s) of total {} byte(s) ...",
                self.pending.len(),
                self.pending.iter().fold(0, |s, ref x| s + x.ulen()));

            match self.send_all() {
                Ok(mut nlen) => {
                    debug!("sent {} bytes", nlen);
                    while nlen > 0 {
                        // We're sure about unwrapping here, because messages are immutable.
                        let bytes_left = self.pending.front().unwrap().ulen();

                        if bytes_left > nlen {
                            self.pending.front_mut().unwrap().eat(nlen);
                            break;
                        }

                        nlen -= bytes_left;
                        self.pending.pop_front().unwrap().complete(Ok(()));
                    }
                }
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(err) => {
                    error!("failed to send bytes: {}", err);
                    for message in self.pending.drain(..) {
                        message.complete(Err(io::Error::last_os_error()));
                    }
                    self.state |= CLOSE_SEND;
                    return Err(err.into());
                }
            }
        }

        Ok(NotReady)
    }

    fn poll_recv(&mut self) -> Poll<(), MultiplexError> {
        loop {
            match self.sock.read(&mut self.ring[self.rd_offset..]) {
                Ok(0) => {
                    self.state |= CLOSE_RECV;

                    if self.dispatches.is_empty() {
                        debug!("EOF");
                    } else {
                        warn!("EOF while there are {} pending dispatch(es)", self.dispatches.len());
                        for (.., dispatch) in self.dispatches.drain() {
                            dispatch.discard(&Error::Io(unexpected_eof()));
                        }
                        return Err(unexpected_eof().into());
                    }

                    return Ok(Ready(()));
                }
                Ok(nread) => {
                    self.rd_offset += nread;
                    debug!("read {} bytes; Ring {{ rx: {}, rd: {}, len: {} }}", nread, self.rx_offset, self.rd_offset, self.ring.len());

                    // Drain the ring until all messages are decoded. Should always be broken with
                    // unexpected EOF error if everything is ok.
                    loop {
                        let mut rdbuf = Cursor::new(&self.ring[self.rx_offset..self.rd_offset]);
                        match read_value_ref(&mut rdbuf) {
                            Ok(raw) => {
                                let frame = Frame::new(&raw)?;
                                debug!("-> {}", frame);
                                // TODO: First check the performance difference between owning &
                                // non-owning values. Then it is possible to easily adapt this code.
                                // Hyper requires ownership, `minihttp` copies bytes.
                                // Value +++:
                                //  - Owned.
                                //  - No lifetimes hell.
                                // Value ---:
                                //  - Copy from readable buffer (1 copy).
                                //
                                // ValueRef +++:
                                //  - Faster, but still requires heap allocation.
                                // ValueRef ---:
                                //  - Lifetimes hell.
                                //  - Still requires to copy to move ownership to Hyper (1 copy,
                                //    but more granular).
                                let id = frame.id();
                                let ty = frame.ty();
                                let args = frame.args();

                                match self.dispatches.remove(&id) {
                                    Some(dispatch) => {
                                        match dispatch.process(ty, args) {
                                            Some(dispatch) => {
                                                self.dispatches.insert(id, dispatch);
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

                                self.rx_offset += rdbuf.position() as usize;
                            }
                            Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => {
                                debug!("failed to decode frame - insufficient bytes");
                                break;
                            }
                            Err(err) => {
                                error!("failed to decode value from the read buffer: {:?}", err);
                                return Err(err.into());
                            }
                        }
                    }

                    let pending = self.rd_offset - self.rx_offset;
                    if self.rx_offset != 0 {
                        unsafe {
                            ptr::copy(
                                self.ring.as_ptr().offset(self.rx_offset as isize),
                                self.ring.as_mut_ptr(),
                                pending
                            );
                        }

                        self.rd_offset = pending;
                        self.rx_offset = 0;
                        debug!("compactified the ring");
                    }

                    let len = self.ring.len();
                    if pending * 2 >= len {
                        // The total size of unprocessed data in larger than half the size of the
                        // ring, so grow the ring in order to accomodate more data.
                        self.ring.resize(len * 2, 0);
                        debug!("resized rdbuf to {}", self.ring.len());
                    }
                }
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(err) => {
                    error!("WTF: {:?}", err);
                    // TODO: Probably we shouldn't return here even if reader part was shut down
                    // to be able to write entirely.
                    // TODO: Toggle `CloseRead` flag.
                    unimplemented!();
                }
            }
        }

        Ok(NotReady)
    }
}

impl<T: Read + Write + AsRawFd> Future for Multiplex<T> {
    type Item = ();
    type Error = MultiplexError;

    fn poll(&mut self) -> Poll<(), Self::Error> {
        // We guarantee that after this future is finished with I/O error it will have no new
        // events. However to be able to gracefully finish all dispatches or sender obsersers we
        // move ownership of this object into the event loop.

        if !self.state.contains(CLOSE_SEND) {
            match self.poll_send() {
                Ok(Ready(())) => return Ok(Ready(())),
                Ok(NotReady) => {}
                Err(err) => return Err(err), // TODO: Discard all pending events here.
            }
        }

        if !self.state.contains(CLOSE_RECV) {
            match self.poll_recv() {
                Ok(Ready(())) => return Ok(Ready(())),
                Ok(NotReady) => {}
                Err(err) => {
                    for (.., dispatch) in self.dispatches.drain() {
                        dispatch.discard(&err.clone().into());
                    }
                    return Err(err);
                }
            }
        }

        if self.state.contains(CLOSE_USER) && self.pending.is_empty() && self.dispatches.is_empty() {
            Ok(Ready(()))
        } else {
            Ok(NotReady)
        }
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

    pub fn send<T>(&self, ty: u64, args: &T)
        where T: Serialize
    {
        let buf = rmps::to_vec(args).unwrap();

        let event = Event::Push {
            id: self.id,
            ty: ty,
            data: buf,
        };
        self.tx.send(event).unwrap();
    }
}

impl Debug for Sender {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Sender").field("id", &self.id).finish()
    }
}

#[derive(Debug)]
pub enum Error {
    /// Operation has been aborted due to I/O error.
    Io(io::Error),
    /// Transport protocol error, for example invalid MessagePack message.
    // TODO: -> TransportProtocolError
    InvalidProtocol(io::Error),
    /// Framing error.
    // TODO: -> ApplicationProtocolError
    InvalidFraming(frame::Error),
    /// Failed to unpack data frame into the expected type.
    InvalidDataFraming(String),
    /// Service error with category, type and optional description.
    Service(u64, u64, Option<String>),
    /// Operation has been canceled internally due to unstoppable forces.
    Canceled,
}

impl Error {
    fn clone(&self) -> Self {
        match *self {
            Error::Io(ref err) => {
                Error::Io(io::Error::new(err.kind(), error::Error::description(err)))
            }
            Error::InvalidProtocol(ref err) => {
                Error::InvalidProtocol(io::Error::new(err.kind(), error::Error::description(err)))
            }
            Error::InvalidFraming(ref err) => {
                Error::InvalidFraming(err.clone())
            }
            Error::InvalidDataFraming(ref err) => Error::InvalidDataFraming(err.clone()),
            Error::Service(cat, ty, ref desc) => Error::Service(cat, ty, desc.clone()),
            Error::Canceled => Error::Canceled,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<frame::Error> for Error {
    fn from(err: frame::Error) -> Error {
        Error::InvalidFraming(err)
    }
}

impl From<MultiplexError> for Error {
    fn from(err: MultiplexError) -> Error {
        match err {
            MultiplexError::Io(err) => Error::Io(err),
            MultiplexError::InvalidProtocol(err) => Error::InvalidProtocol(err),
            MultiplexError::InvalidFraming(err) => Error::InvalidFraming(err),
        }
    }
}

impl Display for Error {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::Io(ref err) => Display::fmt(&err, fmt),
            Error::InvalidProtocol(ref err) => Display::fmt(&err, fmt),
            Error::InvalidFraming(ref err) => Display::fmt(&err, fmt),
            Error::InvalidDataFraming(ref err) => Display::fmt(&err, fmt),
            Error::Service(.., id, None) => write!(fmt, "[{}] no description", id),
            Error::Service(.., id, Some(ref desc)) => write!(fmt, "[{}]: {}", id, desc),
            Error::Canceled => write!(fmt, "canceled"),
        }
    }
}

enum State {
    Disconnected,
    Resolving(Box<Future<Item=Vec<SocketAddr>, Error=Error>>),
    Connecting(Box<Future<Item=TcpStream, Error=io::Error>>),
    Running(Multiplex<TcpStream>),
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

pub trait Resolve {
    fn resolve(&mut self, name: &str) -> Box<Future<Item = Vec<SocketAddr>, Error = Error>>;
}

/// A no-op resolver, that always returns preliminarily specified endpoints.
///
/// Used primarily while resolving a `Locator` itself, but can be also used, when you're sure about
/// service's location.
pub struct FixedResolver {
    addrs: Vec<SocketAddr>,
}

impl FixedResolver {
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        FixedResolver {
            addrs: addrs,
        }
    }
}

impl Default for FixedResolver {
    fn default() -> Self {
        FixedResolver {
            addrs: vec![SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 10053)],
        }
    }
}

impl Resolve for FixedResolver {
    fn resolve(&mut self, _name: &str) -> Box<Future<Item = Vec<SocketAddr>, Error = Error>> {
        box future::ok(self.addrs.clone())
    }
}

struct Resolver {
    locator: Locator,
}

impl Resolver {
    fn new(locator: Service) -> Self {
        Resolver {
            locator: Locator::new(locator),
        }
    }
}

impl Resolve for Resolver {
    fn resolve(&mut self, name: &str) -> Box<Future<Item = Vec<SocketAddr>, Error = Error>> {
        box self.locator.resolve(name).map(|info| info.endpoints().into())
    }
}

#[must_use = "futures do nothing unless polled"]
struct Supervisor<R> {
    // Service name for resolution and debugging.
    name: Cow<'static, str>,
    // Resolver.
    resolver: R,
    // State.
    state: Option<State>,
    // control: unsync::mpsc::UnboundedReceiver<Control>,
    // Event channel.
    rx: Fuse<mpsc::UnboundedReceiver<Event>>,
    // Event loop notifier.
    handle: Handle,
    // TODO: connection_observers: VecDeque<Sender<Result<(), Error>>>,
    // Saved events while not being connected.
    events: VecDeque<Event>,
}

impl<R> Supervisor<R> {
    /// Spawns a supervisor that will work inside the given event loop's context.
    fn spawn(name: Cow<'static, str>, resolver: R, handle: &Handle) -> mpsc::UnboundedSender<Event>
        where R: Resolve + 'static
    {
        let (tx, rx) = mpsc::unbounded();

        let v = Supervisor {
            name: name,
            resolver: resolver,
            state: Some(State::Disconnected),
            rx: rx.fuse(),
            handle: handle.clone(),
            events: VecDeque::new(),
        };

        handle.spawn(v.map_err(|err| warn!("stopped supervisor task: {:?}", err)));

        tx
    }
}

impl<R: Resolve> Future for Supervisor<R> {
    type Item = ();
    type Error = MultiplexError;

    fn poll(&mut self) -> Poll<(), Self::Error> {
        debug!("poll supervisor, state: {} [id={:p}]", self.state.as_ref().unwrap(), self);

        match self.state.take().expect("failed to extract internal state") {
            State::Disconnected => {
                // The only reason we've been woken up here - new events. There can be both
                // invocation and cancellation events.
                match self.rx.poll() {
                    Ok(Ready(Some(event))) => {
                        // TODO: Implement.
                        // 2 ways:
                        //  - 2 channels (1 for control events, 1 for RPC).
                        //      + no match for RPC at not-ready-time.
                        //      - additional channel.
                        //  - 1 channel (1 big match).
                        //      + single channel.
                        //      - 1 match for each event during extract, 1 during queue flush.
                        // match event {
                        //     Event::Connect |
                        //     Event::Reconnect => {
                        //
                        //     }
                        //     // Event::Disconnect => {} // Do nothing.
                        //     event => {} // Push and connect.
                        // }
                        self.events.push_back(event);
                        debug!("pushed event into the queue, pending: {}", self.events.len());

                        self.state = Some(State::Resolving(self.resolver.resolve(&self.name)));
                        debug!("switched state from `disconnected` to `resolving`");
                        return self.poll();
                    }
                    Ok(Ready(None)) | Err(()) => {
                        // Channel between user and us has been closed, and we are not connected.
                        info!("service state machine has been terminated");
                        return Ok(Ready(()));
                    }
                    Ok(NotReady) => {
                        self.state = Some(State::Disconnected);
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
                        error!("failed to resolve `{}` service: {}", self.name, err);
                        for event in self.events.drain(..) {
                            match event {
                                Event::Call { dispatch, .. } => {
                                    dispatch.discard(&err);
                                }
                                Event::Push { .. } => {}
                            }
                        }

                        self.state = Some(State::Disconnected);
                    }
                }
            }
            State::Connecting(mut future) => {
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
                    Ok(Ready(sock)) => {
                        let peer = sock.peer_addr()?;
                        sock.set_nodelay(true)?;

                        info!("successfully connected to {}", peer);
                        let mut mx = Multiplex::new(sock, peer);

                        for event in self.events.drain(..) {
                            mx.add_event(event);
                        }

                        self.state = Some(State::Running(mx));
                        return self.poll();
                    }
                    Ok(NotReady) => {
                        debug!("connection - in progress");
                        self.state = Some(State::Connecting(future));
                    }
                    Err(err) => {
                        error!("failed to connect to `{}` service: {}", self.name, err);
                        let err = Error::Io(err);
                        for event in self.events.drain(..) {
                            match event {
                                Event::Call { dispatch, .. } => {
                                    dispatch.discard(&err);
                                }
                                Event::Push { .. } => {}
                            }
                        }
                        self.state = Some(State::Disconnected);
                    }
                }
            }
            State::Running(mut future) => {
                loop {
                    match self.rx.poll() {
                        Ok(Ready(Some(event))) => {
                            future.add_event(event);
                        }
                        Ok(NotReady) => {
                            break;
                        }
                        Ok(Ready(None)) | Err(()) => {
                            // No more user input.
                            if future.pending.len() > 0 || future.dispatches.len() > 0 {
                                debug!("detached supervisor with {} messages and {} dispatches",
                                    future.pending.len(), future.dispatches.len());

                                future.state |= CLOSE_USER;
                                self.handle.spawn(future.map_err(|_err| ()));
                            }
                            return Ok(Ready(()));
                        }
                    }
                }

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

/// An entry point to the Cocaine Cloud.
///
/// The `Service` provides an ability to work with Cocaine services in a thread-safe manner with
/// automatic name resolution and reconnection before the next request after any unexpected
/// I/O error.
///
/// Internally it has an asynchronous state machine, which runs in an event loop associated with
/// the handle given at construction time.
#[derive(Clone)]
pub struct Service {
    name: Cow<'static, str>,
    tx: mpsc::UnboundedSender<Event>,
}

impl Service {
    /// Constructs a new `Service` with a given name.
    ///
    /// This will not perform a connection attempt until required - both name resolution and
    /// connection will be performed on demand. You can call `connect()` method for fine-grained
    /// control.
    ///
    /// For more fine-grained service configuration use `cocaine::service::Builder` instead.
    pub fn new<N>(name: N, handle: &Handle) -> Self
        where N: Into<Cow<'static, str>>
    {
        Builder::new(name, handle.clone()).build()
    }

    /// Returns service name.
    pub fn name(&self) -> &str {
        &self.name
    }

    // TODO: pub fn `connect(&self)`.
    // TODO: pub fn `disconnect(&self)`.

    pub fn call<T, D>(&self, ty: u64, args: &T, dispatch: D) -> impl Future<Item = Sender, Error = Error>
        where T: Serialize,
              D: Dispatch + Send + 'static
    {
        let buf = rmps::to_vec(args).unwrap();

        let (tx, rx) = oneshot::channel();
        let event = Event::Call {
            ty: ty,
            data: buf,
            dispatch: box dispatch,
            tx: tx
        };
        self.tx.send(event).unwrap();
        let tx = self.tx.clone();

        rx.then(|send| {
            match send {
                Ok(Ok(id)) => Ok(Sender::new(id, tx)),
                Ok(Err(err)) => Err(err),
                Err(Canceled) => Err(Error::Canceled),
            }
        })
    }
}

impl Debug for Service {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Service")
            .field("name", &self.name)
            .finish()
    }
}

fn _assert_service_sync_send() {
    fn _assert<T: Sync + Send>() {}
    _assert::<Service>();
}
