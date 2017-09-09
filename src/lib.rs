//! Roadmap:
//!
//! - [x] Unicorn wrapper.
//! - [x] Send headers.
//! - [x] Notify about send events completion.
//! - [ ] Infinite buffer growing protection.
//! - [x] Implement `local_addr` and `peer_addr` for `Service`.
//! - [ ] Generic multiplexer over the socket type, allowing to work with both TCP and Unix sockets.
//! - [ ] Receiving headers.
//! - [ ] HPACK encoder.
//! - [ ] HPACK decoder.

#![feature(conservative_impl_trait)]

#![warn(missing_docs, missing_debug_implementations)]

#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate futures;
extern crate libc;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate rmpv;
extern crate tokio_core;
extern crate tokio_io;

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{self, Cursor, ErrorKind, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::ptr;
use std::sync::{Arc, Mutex};

use futures::{Async, Future, Poll, Stream};
use futures::stream::Fuse;
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::sync::oneshot;

use tokio_core::net::TcpStream;
pub use tokio_core::reactor::Core;
use tokio_core::reactor::Handle;
use tokio_io::io::Window;

use rmpv::ValueRef;
use rmpv::decode::read_value_ref;

use Async::*;

pub mod dispatch;
mod frame;
pub mod hpack;
pub mod logging;
mod net;
pub mod protocol;
mod resolve;
mod request;
pub mod service;
mod sys;

use net::connect;
use self::frame::Frame;
use self::hpack::RawHeader;
pub use self::resolve::{FixedResolver, Resolve, Resolver};
pub use self::request::Request;
pub use self::service::ServiceBuilder;
pub use self::service::locator::EventGraph;
use self::sys::{PollWrite, SendAll};

const FRAME_LENGTH: u32 = 4;

/// Receiver part of every multiplexed non-mute request performed with a service.
///
/// Implementors of this trait are used to be passed into the [`Service.call`][call] method to
/// accumulate response chunks.
///
/// It is guaranteed that at least one of the [`process`][process] or [`discard`][discard] methods
/// will be called at least once during a channel lifetime.
/// Note, that [`discard`][discard] method can be called no more than once.
///
/// [call]: struct.Service.html#method.call
/// [process]: #method.process
/// [discard]: #method.discard
pub trait Dispatch: Send {
    /// Processes a new incoming message from a service.
    ///
    /// This method is called on every valid frame received from a service for an associated
    /// channel with message type and arguments provided. Usually the next step performed is
    /// arguments deserialization using [`deserialize`][deserialize] function.
    ///
    /// Passing `Some(..)` as a result type forces the multiplexer to re-register either new or the
    /// same `Dispatch` for processing new messages from the same channel again. Returning `None`
    /// terminates channel processing.
    /// It is also possible to completely switch dispatches at runtime, because of dynamic
    /// dispatching.
    ///
    /// [deserialize]: protocol/fn.deserialize.html
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>>;

    /// Discards the dispatch due to some error occurred during receiving the response.
    ///
    /// This is the terminate state of any dispatch call graph. No more `Dispatch` calls will be
    /// performed, because this method accepts a boxed dispatch by value.
    fn discard(self: Box<Self>, err: &Error);
}

/// Helper mapping function that is used in conjunction with `then` combinator when returning
/// oneshot sender to move `oneshot::Canceled` error into the standard one while unwrapping the
/// nested error.
fn flatten_err<T, E>(result: Result<Result<T, Error>, E>) -> Result<T, Error>
    where E: Into<Error>
{
    match result {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(e.into()),
    }
}

struct Call {
    request: Request,
    dispatch: Box<Dispatch>,
    complete: oneshot::Sender<Result<u64, Error>>,
}

impl Debug for Call {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Call")
            .field("request", &self.request)
            .finish()
    }
}

impl Into<MultiplexEvent> for Call {
    fn into(self) -> MultiplexEvent {
        MultiplexEvent::Call(self)
    }
}

struct Mute {
    request: Request,
    complete: oneshot::Sender<Result<u64, Error>>,
}

impl Debug for Mute {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Mute")
            .field("request", &self.request)
            .finish()
    }
}

impl Into<MultiplexEvent> for Mute {
    fn into(self) -> MultiplexEvent {
        MultiplexEvent::Mute(self)
    }
}

struct Push {
    id: u64,
    request: Request,
    complete: oneshot::Sender<Result<(), Error>>,
}

impl Debug for Push {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Push")
            .field("id", &self.id)
            .field("request", &self.request)
            .finish()
    }
}

impl Into<MultiplexEvent> for Push {
    fn into(self) -> MultiplexEvent {
        MultiplexEvent::Push(self)
    }
}

#[derive(Debug)]
enum MultiplexEvent {
    Call(Call),
    Mute(Mute),
    Push(Push),
}

struct MessageBuf {
    head: Window<[u8; 32]>,
    data: Window<Vec<u8>>,
}

impl MessageBuf {
    fn new(id: u64, request: Request) -> Result<Self, io::Error> {
        let mut head = [0; 32];
        let head_len = MessageBuf::encode_head(&mut head[..], id, request.ty())?;
        let mut head = Window::new(head);
        head.set_end(head_len);

        let (mut data, headers) = request.into_components();
        MessageBuf::encode_headers(&mut data, headers)?;

        let mbuf = MessageBuf {
            head: head,
            data: Window::new(data),
        };

        Ok(mbuf)
    }

    fn encode_head(head: &mut [u8], id: u64, ty: u64) -> Result<usize, io::Error> {
        let mut cur = Cursor::new(&mut head[..]);
        rmp::encode::write_array_len(&mut cur, FRAME_LENGTH)?;
        rmp::encode::write_uint(&mut cur, id)?;
        rmp::encode::write_uint(&mut cur, ty)?;

        Ok(cur.position() as usize)
    }

    fn encode_headers<W>(wr: &mut W, headers: Vec<RawHeader>) -> Result<(), io::Error>
        where W: Write
    {
        rmp::encode::write_array_len(wr, headers.len() as u32)?;
        for header in headers {
            rmp::encode::write_array_len(wr, 3)?;
            // Explicitly mark that the remote side should not put headers in the dynamic table.
            rmp::encode::write_bool(wr, false)?;
            rmp::encode::write_bin(wr, &header.name[..])?;
            rmp::encode::write_bin(wr, &header.data[..])?;
        }

        Ok(())
    }

    fn remaining(&self) -> usize {
        self.head.as_ref().len() + self.data.as_ref().len()
    }

    /// Advance the internal cursor of the `MessageBuf`.
    ///
    /// # Panics
    ///
    /// This method will panic if `num` is out of bounds for the underlying slice or if it comes
    /// after the end configured in this message.
    fn advance(&mut self, num: usize) {
        let mut num = num;

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
    Push(oneshot::Sender<Result<(), Error>>),
}

impl Notify {
    fn complete(self, val: Result<(), io::Error>) {
        match self {
            Notify::Call(id, tx) => drop(tx.send(val.and(Ok(id)).map_err(Error::Io))),
            Notify::Push(tx) => drop(tx.send(val.map_err(Error::Io))),
        }
    }
}

struct Message {
    mbuf: MessageBuf,
    notify: Notify,
}

impl Message {
    /// Unwritten length.
    fn remaining(&self) -> usize {
        self.mbuf.remaining()
    }

    fn advance(&mut self, n: usize) {
        self.mbuf.advance(n)
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
                MultiplexError::InvalidFraming(*err)
            }
        }
    }
}

impl From<io::Error> for MultiplexError {
    fn from(err: io::Error) -> Self {
        MultiplexError::Io(err)
    }
}

impl From<rmpv::decode::Error> for MultiplexError {
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
/// no more dispatches left.
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
    ErrorKind::UnexpectedEof.into()
}

impl<T: Read + Write + SendAll + PollWrite> Multiplex<T> {
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

    fn add_event(&mut self, event: MultiplexEvent) {
        match event {
            MultiplexEvent::Call(Call { request, dispatch, complete }) => {
                self.invoke(request, complete);
                self.dispatches.insert(self.id, dispatch);
            }
            MultiplexEvent::Mute(Mute { request, complete }) => {
                self.invoke(request, complete);
            }
            MultiplexEvent::Push(Push { id, request, complete }) => {
                self.push(id, request, || Notify::Push(complete))
            }
        }
    }

    fn invoke(&mut self, request: Request, complete: oneshot::Sender<Result<u64, Error>>) {
        self.id += 1;
        let id = self.id;
        self.push(id, request, || Notify::Call(id, complete));
    }

    fn push<F>(&mut self, id: u64, request: Request, f: F)
        where F: FnOnce() -> Notify
    {
        let mbuf = MessageBuf::new(id, request).expect("failed to pack frame header");
        let message = Message {
            mbuf: mbuf,
            notify: f(),
        };
        self.pending.push_back(message);
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

        SendAll::send_all(&mut self.sock, &bufs[..size])
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
                self.pending.iter().fold(0, |s, x| s + x.remaining()));

            match self.send_all() {
                Ok(mut nlen) => {
                    debug!("sent {} bytes", nlen);
                    while nlen > 0 {
                        // We're sure about unwrapping here, because messages are immutable.
                        let bytes_left = self.pending.front().unwrap().remaining();

                        if bytes_left > nlen {
                            self.pending.front_mut().unwrap().advance(nlen);
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
                        // ring, so grow the ring in order to accommodate more data.
                        self.ring.resize(len * 2, 0);
                        debug!("resized rdbuf to {}", self.ring.len());
                    }
                }
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(err) => {
                    error!("failed to read from the socket: {:?}", err);

                    self.state |= CLOSE_RECV;

                    let e = io::Error::last_os_error().into();
                    for (.., dispatch) in self.dispatches.drain() {
                        dispatch.discard(&e);
                    }
                    return Err(err.into());
                }
            }
        }

        Ok(NotReady)
    }
}

impl<T: Read + Write + SendAll + PollWrite> Future for Multiplex<T> {
    type Item = ();
    type Error = MultiplexError;

    fn poll(&mut self) -> Poll<(), Self::Error> {
        // We guarantee that after this future is finished with I/O error it will have no new
        // events. However to be able to gracefully finish all dispatches or sender observers we
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

/// An upstream for sending additional chunks into a channel.
pub struct Sender {
    id: u64,
    tx: UnboundedSender<Event>,
}

impl Sender {
    fn new(id: u64, tx: UnboundedSender<Event>) -> Self {
        Sender { id: id, tx: tx }
    }

    /// Sends a new data chunk into the channel associated with this upstream.
    ///
    /// Returns a future that is completed when a chunk is completely written into the underlying
    /// socket or some error occurred.
    ///
    /// This is the lowest-level API for working with services. You'd probably like to use one of
    /// the provided wrappers, like [App][app] or [Unicorn][unicorn].
    ///
    /// [App]: service/app/struct.App.html
    /// [Unicorn]: service/unicorn/struct.Unicorn.html
    pub fn send(&self, request: Request) -> impl Future<Item = (), Error = Error> {
        let (tx, rx) = oneshot::channel();

        let event = Push {
            id: self.id,
            request: request,
            complete: tx,
        };
        self.tx.unbounded_send(Event::Push(event)).unwrap();

        rx.then(flatten_err)
    }
}

impl Debug for Sender {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Sender").field("id", &self.id).finish()
    }
}

/// An `Error` that can occur while working with `Service`.
#[derive(Debug)]
pub enum Error {
    /// Operation has been aborted due to I/O error.
    Io(io::Error),
    /// Transport protocol error, for example invalid MessagePack message.
    InvalidProtocol(io::Error),
    /// Framing error.
    InvalidFraming(frame::Error),
    /// Failed to unpack data frame into the expected type.
    InvalidDataFraming(String),
    /// Service error with category, type and optional description.
    Service(protocol::Error),
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
                Error::InvalidFraming(*err)
            }
            Error::InvalidDataFraming(ref err) => Error::InvalidDataFraming(err.clone()),
            Error::Service(ref err) => Error::Service(err.clone()),
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

impl From<oneshot::Canceled> for Error {
    fn from(err: oneshot::Canceled) -> Self {
        match err {
            oneshot::Canceled => Error::Canceled
        }
    }
}

impl Display for Error {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::Io(ref err) |
            Error::InvalidProtocol(ref err) => Display::fmt(&err, fmt),
            Error::InvalidFraming(ref err) => Display::fmt(&err, fmt),
            Error::InvalidDataFraming(ref err) => Display::fmt(&err, fmt),
            Error::Service(ref err) => Display::fmt(&err, fmt),
            Error::Canceled => write!(fmt, "canceled"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(..) => "operation has been aborted due to I/O error",
            Error::InvalidProtocol(..) => "transport protocol error",
            Error::InvalidFraming(..) => "invalid framing",
            Error::InvalidDataFraming(..) =>
                "failed to unpack data frame into the expected type",
            Error::Service(..) => "service error",
            Error::Canceled => "operation has been canceled",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Io(ref err) |
            Error::InvalidProtocol(ref err) => Some(err),
            Error::InvalidFraming(ref err) => Some(err),
            Error::InvalidDataFraming(..) |
            Error::Service(..) |
            Error::Canceled => None,
        }
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidDataFraming(format!("{}", msg))
    }
}

enum State<R: Resolve> {
    Disconnected,
    Resolving(R::Future),
    Connecting(Box<Future<Item=TcpStream, Error=io::Error>>),
    Running(Multiplex<TcpStream>),
}

impl<R: Resolve> Debug for State<R> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            State::Disconnected => write!(fmt, "State::Disconnected"),
            State::Resolving(..) => write!(fmt, "State::Resolving"),
            State::Connecting(..) => write!(fmt, "State::Connecting"),
            State::Running(..) => write!(fmt, "State::Running"),
        }
    }
}

impl<R: Resolve> Display for State<R> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            State::Disconnected => write!(fmt, "disconnected"),
            State::Resolving(..) => write!(fmt, "resolving"),
            State::Connecting(..) => write!(fmt, "connecting"),
            State::Running(..) => write!(fmt, "running"),
        }
    }
}

enum Event {
    Connect(oneshot::Sender<Result<(), Error>>),
    Disconnect,
    Call(Call),
    Mute(Mute),
    Push(Push),
}

impl Debug for Event {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Event::Connect(..) => {
                fmt.debug_struct("Event::Connect")
                    .finish()
            }
            Event::Disconnect => fmt.debug_struct("Event::Disconnect").finish(),
            Event::Call(Call { ref request, .. }) => {
                fmt.debug_struct("Event::Call")
                    .field("request", &request)
                    .finish()
            }
            Event::Mute(Mute { ref request, .. }) => {
                fmt.debug_struct("Event::Mute")
                    .field("request", &request)
                    .finish()
            }
            Event::Push(Push { id, ref request, .. }) => {
                fmt.debug_struct("Event::Push")
                    .field("id", &id)
                    .field("request", &request)
                    .finish()
            }
        }
    }
}

#[must_use = "futures do nothing unless polled"]
struct Supervisor<R: Resolve> {
    // Service name for resolution and debugging.
    name: Cow<'static, str>,
    shared: Arc<Mutex<SharedState>>,
    // Resolver.
    resolver: R,
    // State.
    state: Option<State<R>>,
    // Event channel from users.
    rx: Fuse<UnboundedReceiver<Event>>,
    // Event loop notifier.
    handle: Handle,
    // Connection requests.
    concerns: VecDeque<oneshot::Sender<Result<(), Error>>>,
    // Saved events while not being connected.
    events: VecDeque<MultiplexEvent>,
}

impl<R: Resolve> Supervisor<R> {
    /// Spawns a supervisor that will work inside the given event loop's context.
    fn spawn(name: Cow<'static, str>, shared: Arc<Mutex<SharedState>>, resolver: R, handle: &Handle) -> UnboundedSender<Event>
        where R: 'static
    {
        let (tx, rx) = mpsc::unbounded();

        let v = Supervisor {
            name: name,
            shared: shared,
            resolver: resolver,
            state: Some(State::Disconnected),
            rx: rx.fuse(),
            handle: handle.clone(),
            concerns: VecDeque::new(),
            events: VecDeque::new(),
        };

        handle.spawn(v.map_err(|err| warn!("stopped supervisor task: {:?}", err)));

        tx
    }

    #[inline]
    fn push_event<E: Into<MultiplexEvent>>(&mut self, event: E) {
        self.events.push_back(event.into());
        debug!("pushed event into the queue, pending: {}", self.events.len());
    }

    fn disconnect(&mut self) {
        *self.shared.lock().unwrap() = Default::default();
        self.state = Some(State::Disconnected);
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
                        match event {
                            Event::Connect(tx) => {
                                self.concerns.push_back(tx);
                            }
                            Event::Disconnect => {
                                return self.poll();
                            }
                            Event::Call(event) => {
                                self.push_event(event);
                            }
                            Event::Mute(event) => {
                                self.push_event(event);
                            }
                            Event::Push(event) => {
                                self.push_event(event);
                            }
                        }

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
                            match event {
                                Event::Connect(tx) => {
                                    self.concerns.push_back(tx);
                                }
                                Event::Disconnect => {}
                                Event::Call(event) => {
                                    self.push_event(event);
                                }
                                Event::Mute(event) => {
                                    self.push_event(event);
                                }
                                Event::Push(event) => {
                                    self.push_event(event);
                                }
                            }
                        }
                        Ok(..) | Err(()) => {
                            // No new events will be delivered, because there are no more senders,
                            // however we're ok with fire-and-forget strategy, so move forward.
                            break;
                        }
                    }
                }

                match future.poll() {
                    Ok(Ready(info)) => {
                        info!("successfully resolved `{}` service", self.name);

                        let (addrs, methods) = info.into_components();
                        self.shared.lock().unwrap().methods = methods;
                        self.state = Some(State::Connecting(Box::new(connect(addrs, &self.handle))));
                        return self.poll();
                    }
                    Ok(NotReady) => {
                        self.state = Some(State::Resolving(future));
                    }
                    Err(err) => {
                        error!("failed to resolve `{}` service: {}", self.name, err);
                        for concern in self.concerns.drain(..) {
                            drop(concern.send(Err(err.clone())));
                        }

                        for event in self.events.drain(..) {
                            match event {
                                MultiplexEvent::Call(Call { dispatch, complete, .. }) => {
                                    // TODO: Return Error::FailedResolve(err.into()).
                                    dispatch.discard(&err);
                                    drop(complete.send(Err(err.clone())));
                                }
                                MultiplexEvent::Mute(..) |
                                MultiplexEvent::Push(..) => {}
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
                            match event {
                                Event::Connect(tx) => {
                                    self.concerns.push_back(tx);
                                }
                                Event::Disconnect => {}
                                Event::Call(event) => {
                                    self.push_event(event);
                                }
                                Event::Mute(event) => {
                                    self.push_event(event);
                                }
                                Event::Push(event) => {
                                    self.push_event(event);
                                }
                            }
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

                        let local_addr = sock.local_addr()?;

                        info!("successfully connected to {}", peer);

                        for concern in self.concerns.drain(..) {
                            drop(concern.send(Ok(())));
                        }

                        let mut mx = Multiplex::new(sock, peer);

                        for event in self.events.drain(..) {
                            mx.add_event(event);
                        }

                        self.shared.lock().unwrap().peer_addr = Some(peer);
                        self.shared.lock().unwrap().local_addr = Some(local_addr);
                        self.state = Some(State::Running(mx));
                        return self.poll();
                    }
                    Ok(NotReady) => {
                        debug!("connection - in progress");
                        self.state = Some(State::Connecting(future));
                    }
                    Err(err) => {
                        error!("failed to connect to `{}` service: {}", self.name, err);
                        // TODO: Return `Error::FailedConnection(..)` to be able to distinguish
                        // between connection errors and I/O while being connected.
                        let err = Error::Io(err);

                        for concern in self.concerns.drain(..) {
                            drop(concern.send(Err(err.clone())));
                        }

                        for event in self.events.drain(..) {
                            match event {
                                MultiplexEvent::Call(Call { dispatch, .. }) => {
                                    dispatch.discard(&err);
                                }
                                MultiplexEvent::Mute(..) |
                                MultiplexEvent::Push(..) => {}
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
                            match event {
                                Event::Connect(tx) => {
                                    // We're already connected, resolve immediately.
                                    drop(tx.send(Ok(())));
                                }
                                Event::Disconnect => {
                                    self.disconnect();
                                    break;
                                }
                                Event::Call(event) => {
                                    future.add_event(event.into());
                                }
                                Event::Mute(event) => {
                                    future.add_event(event.into());
                                }
                                Event::Push(event) => {
                                    future.add_event(event.into());
                                }
                            }
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
                        self.disconnect();
                    }
                    Ok(NotReady) => {
                        self.state = Some(State::Running(future));
                        debug!("running - not ready");
                    }
                    Err(..) => {
                        // TODO: Notify somebody about error.
                        self.disconnect();
                    }
                }
            }
        }

        Ok(NotReady)
    }
}

#[derive(Default)]
struct SharedState {
    peer_addr: Option<SocketAddr>,
    local_addr: Option<SocketAddr>,
    methods: Option<HashMap<u64, EventGraph>>,
}

/// A low-level entry point to the Cocaine Cloud.
///
/// The `Service` provides an ability to work with Cocaine services in a thread-safe manner with
/// automatic name resolution and reconnection before the next request after any unexpected
/// I/O error.
///
/// Internally it has an asynchronous state machine, which runs in an event loop associated with
/// the handle given at construction time.
///
/// Most time you find out that it's more practical to use convenient service wrappers, like
/// [`Locator`][Locator], [`Unicorn`][Unicorn] etc.
///
/// [Locator]: service/locator/struct.Locator.html
/// [Unicorn]: service/unicorn/struct.Unicorn.html
#[derive(Clone)]
pub struct Service {
    name: Cow<'static, str>,
    shared: Arc<Mutex<SharedState>>,
    tx: UnboundedSender<Event>,
}

impl Service {
    /// Constructs a new `Service` with a given name.
    ///
    /// This will not perform a connection attempt until required - both name resolution and
    /// connection will be performed on demand, but you can still call [`connect`][connect] method
    /// to perform connection attempt.
    ///
    /// For more fine-grained service configuration use [`ServiceBuilder`][builder] instead.
    ///
    /// [connect]: #method.connect
    /// [builder]: service/struct.ServiceBuilder.html
    pub fn new<N>(name: N, handle: &Handle) -> Self
        where N: Into<Cow<'static, str>>
    {
        ServiceBuilder::new(name).build(handle)
    }

    /// Returns service name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Connects to the service, performing name resolution and TCP connection establishing.
    ///
    /// Does nothing, if a service is already connected to some backend.
    ///
    /// Usually a service connects automatically on demand, however it may be useful to optimize
    /// away a connection delay by doing pre-connection using this method.
    pub fn connect(&self) -> impl Future<Item=(), Error=Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Event::Connect(tx)).unwrap();
        rx.then(flatten_err)
    }

    /// Returns methods map if available.
    ///
    /// The return value can be `None` if either the service is not connected or the `Resolve` has
    /// provided incomplete information.
    /// Note that this method aren't meant to be used to check whether the service is connected,
    /// because it can return valid methods map, while still connecting to real endpoint. To check
    /// the connection status use [`peer_addr`][peer_addr] method instead.
    ///
    /// [peer_addr]: #method.peer_addr
    pub fn methods(&self) -> Option<HashMap<u64, EventGraph>> {
        self.shared.lock().unwrap().methods.clone()
    }

    /// Disconnects from a remote service without discarding pending requests.
    pub fn disconnect(&self) {
        self.tx.unbounded_send(Event::Disconnect).expect("communication channel must live");
    }

    /// Returns the socket address of the remote peer of this TCP connection.
    ///
    /// Returns an I/O error with `ErrorKind::NotConnected` if this `Service` is not currently
    /// connected.
    pub fn peer_addr(&self) -> Result<SocketAddr, io::Error> {
        self.shared.lock().unwrap().peer_addr.ok_or_else(|| ErrorKind::NotConnected.into())
    }

    /// Returns the socket address of the local half of this TCP connection.
    ///
    /// Returns an I/O error with `ErrorKind::NotConnected` if this `Service` is not currently
    /// connected.
    pub fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        self.shared.lock().unwrap().local_addr.ok_or_else(|| ErrorKind::NotConnected.into())
    }

    /// Performs an RPC with a specified type and arguments.
    ///
    /// The result type is a future of `Sender`, because service requires TCP connection
    /// established before the request can be processed.
    ///
    /// # Warning
    ///
    /// Calling a **mute** event using this method essentially leads to memory leak during this
    /// object's entire lifetime, since the specified `Dispatch` will be captured.
    /// For mute RPC use [`call_mute`][call_mute] instead.
    ///
    /// [call_mute]: #method.call_mute
    pub fn call<D>(&self, request: Request, dispatch: D) -> impl Future<Item=Sender, Error=Error>
    where
        D: Dispatch + 'static
    {
        let (tx, rx) = oneshot::channel();
        let event = Call {
            request: request,
            dispatch: Box::new(dispatch),
            complete: tx,
        };
        self.tx.unbounded_send(Event::Call(event)).unwrap();

        let tx = self.tx.clone();

        rx.then(flatten_err).and_then(|id| Ok(Sender::new(id, tx)))
    }

    /// Performs a mute RPC with a specified type and arguments.
    ///
    /// Mute calls have no responses, that's why this method does not require a dispatch.
    ///
    /// # Warning
    ///
    /// Calling a service event, that actually does respond, leads to silent dropping all received
    /// response chunks.
    pub fn call_mute(&self, request: Request) -> impl Future<Item=Sender, Error=Error> {
        let (tx, rx) = oneshot::channel();
        let event = Mute {
            request: request,
            complete: tx,
        };
        self.tx.unbounded_send(Event::Mute(event)).unwrap();

        let tx = self.tx.clone();

        rx.then(flatten_err).and_then(|id| Ok(Sender::new(id, tx)))
    }
}

impl Debug for Service {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Service")
            .field("name", &self.name)
            .finish()
    }
}

fn _assert_kinds() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_clone<T: Clone>() {}

    _assert_send::<Service>();
    _assert_sync::<Service>();
    _assert_clone::<Service>();
}
