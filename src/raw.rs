//! Contains structs and traits that work with raw protocol.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;

use rmp;
use rmp::Value;
use rmp_serde::Serializer;
use serde::{Serialize};

use super::Error;
use protocol::{Frame};

/// Represents the dispatcher control protocol.
enum Control {
    /// Registers the new channel with the given id and sender associated with.
    Invoke(u64, mpsc::Sender<Result<(u64, Value), Error>>),
    /// Revokes previously registered channel.
    Revoke(u64),
    /// Dispatches the new incoming event.
    Dispatch(Frame),
    /// Notifies that `Service` can no longer do its job. This value should only be sent from the
    /// reader thread.
    Shutdown(Error),
}

impl ::std::fmt::Debug for Control {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Control::Invoke(id, _) => {
                write!(f, "Control::Invoke({})", id)
            }
            Control::Revoke(id) => {
                write!(f, "Control::Revoke({})", id)
            }
            Control::Dispatch(ref frame) => {
                write!(f, "Control::Dispatch({:?})", frame)
            }
            Control::Shutdown(..) => {
                write!(f, "Control::Shutdown")
            }
        }
    }
}

enum WriteEvent {
    /// Pushes bytes through network.
    Append(Vec<u8>),
    /// Pushes bytes through network and notifies after operation completion.
    AppendWait(Vec<u8>, mpsc::Sender<Result<(), ::std::io::Error>>),
}

pub struct Sender {
    id: u64,
    tx: mpsc::Sender<WriteEvent>,
}

impl Sender {
    pub fn send<A>(&self, ty: u64, args: &A) -> Result<(), Error>
        where A: Serialize
    {
        let mut buf = Vec::new();
        (self.id, ty, args).serialize(&mut Serializer::new(&mut buf)).unwrap();

        debug!("push type {} method with {} span", ty, self.id);
        trace!("<- {:?}", buf);

        let (send, wait) = mpsc::channel();
        self.tx.send(WriteEvent::AppendWait(buf, send)).unwrap();

        wait.recv().unwrap().map_err(From::from)
    }
}

/// Represents the stream reader watcher.
///
/// Its the only purpose is to track whether there anyone who can read from the stream and to close
/// the reader part otherwise.
/// Meant to be shared across the `Service` and its `Receiver` instances.
struct Watcher(TcpStream);

impl Drop for Watcher {
    fn drop(&mut self) {
        if let Err(err) = self.0.shutdown(::std::net::Shutdown::Read) {
            warn!("unable to shutdown reader side for transport stream: {}", err);
        }
    }
}

pub struct Receiver {
    id: u64,
    tx: mpsc::Sender<Control>,
    rx: mpsc::Receiver<Result<(u64, Value), Error>>,

    _watcher: Arc<Watcher>,
}

impl Receiver {
    pub fn recv(&self) -> Result<(u64, Value), Error> {
        // NOTE: The dispatcher thread must be alive, so this operation is safe.
        self.rx.recv().unwrap()
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
       self.tx.send(Control::Revoke(self.id)).unwrap();
    }
}

pub struct Service {
    /// TCP transport sender.
    writer: mpsc::Sender<WriteEvent>,

    /// Dispatch sender.
    tx: mpsc::Sender<Control>,

    /// Channel id counter.
    counter: u64,

    /// Worker threads.
    ///
    /// There are three worker threads per service instance: writer, reader and event dispatcher.
    /// We need a separate thread for writing events, because:
    /// - Avoid race conditions around mixing `write_all` calls.
    /// - Separate receiving and sending bytes.
    /// - This saves us both from channel number races and from data mixing race (when multiple
    /// threads are unable to write the entire frame in a single syscall). Also this automatically
    /// makes `Send` for `Service` for some cases.
    threads: Option<(JoinHandle<()>, JoinHandle<()>, JoinHandle<()>)>,

    watcher: Arc<Watcher>,
}

impl Service {
    pub fn connect(endpoint: &SocketAddr) -> Result<Service, Error> {
        let stream = try!(TcpStream::connect(endpoint));

        let (tx, rx) = mpsc::channel();
        let (tw, rw) = mpsc::channel();

        let threads = {
            let writer = {
                let stream = try!(stream.try_clone());
                thread::spawn(move || Service::process_write(stream, rw))
            };

            let reader = {
                let tx = tx.clone();
                let stream = try!(stream.try_clone());
                thread::spawn(move || Service::process_read(stream, tx))
            };

            let dispatch = thread::spawn(move || Service::dispatch(rx));

            (writer, reader, dispatch)
        };

        let watcher = Watcher(try!(stream.try_clone()));

        let service = Service {
            writer: tw,
            tx: tx,
            counter: 0,
            threads: Some(threads),
            watcher: Arc::new(watcher),
        };

        Ok(service)
    }

    // TODO: How to make `Service` to implement `Send`. There are three possible solutions:
    // I. Lock fucking all (may be done externally).
    // ++ Args can be passed by ref and not required to be `Send`.
    // +  Single buffer allocation, but every time.
    // -- Locking.
    //
    // II. Using deferred serialization.
    // 1. Serialize using callback == args: Send.
    // 2. Invoke(id, ty, args, tx).
    // 3. id.recv() - block until id++ or until message sent.
    // ++ Single buffer allocation, single time, but growable (this is great!).
    // -  Serialization load on writer thread.
    // -- Args must be passed by value, required to be Send (unnecessary bytes copy).
    //
    // III. Using RMP magic.
    // 1. Serialize only args.
    // 2. Invoke(id, ty, args, tx).
    // 3. Serialize id, ty ++ args using low level methods.
    // 4. id.recv().
    // ++ Args can be passed by ref and not required to be `Send`.
    // -- Double buffer allocation, every time.
    // -  Less, but non-zero serialization load on writer thread.
    // TODO: Need benchmarks to prove what's better.
    pub fn invoke<A>(&mut self, ty: u64, args: &A) -> Result<(Sender, Receiver), Error>
        where A: Serialize
    {
        let mut id = &mut self.counter;
        *id += 1;

        // Serialize arguments.
        let mut buf = Vec::new();
        (*id, ty, args).serialize(&mut Serializer::new(&mut buf)).unwrap();

        debug!("invocation type {} method with {} span", ty, *id);
        trace!("<= {:?}", buf);

        let (tx, rx) = mpsc::channel();
        self.tx.send(Control::Invoke(*id, tx)).unwrap();

        let (send, wait) = mpsc::channel();
        self.writer.send(WriteEvent::AppendWait(buf, send)).unwrap();

        let tx = Sender {
            id: *id,
            tx: self.writer.clone(),
        };

        let rx = Receiver {
            id: *id,
            tx: self.tx.clone(),
            rx: rx,
            _watcher: self.watcher.clone(),
        };

        try!(wait.recv().unwrap());

        Ok((tx, rx))
    }

    // TODO: Invoke mute method.
    // Invokes a mute event.
    // fn invoke_mute<A>(&mut self, ty: u64, args: &A) -> Result<Sender, Error>;

    fn process_write(mut stream: TcpStream, rx: mpsc::Receiver<WriteEvent>) {
        // This loop will exhause only when there are no `Service` and `Sender` instances alive.
        for event in rx {
            match event {
                WriteEvent::Append(data) => {
                    // TODO: Write all.
                    unimplemented!();
                }
                WriteEvent::AppendWait(data, tx) => {
                    tx.send(stream.write_all(&data)).unwrap();
                }
            }
        }

        if let Err(err) = stream.shutdown(::std::net::Shutdown::Write) {
            warn!("unable to shutdown writer side for transport stream: {}", err);
        }
    }

    // NOTE: It's safe to unwrap result of `send` method for this function, because it's guaranteed
    // that this thread outlives the dispatcher thread.
    fn process_read(stream: TcpStream, tx: mpsc::Sender<Control>) {
        let mut rd = BufReader::with_capacity(4096, stream);

        loop {
            debug!("waiting for new data ...");
            let nread = match rd.fill_buf() {
                Ok([]) => {
                    use std::io::ErrorKind;
                    debug!("-> EOF");

                    tx.send(Control::Shutdown(Error::Io(::std::io::Error::new(ErrorKind::Other, "EOF")))).unwrap();
                    break;
                }
                Ok(buf) => {
                    trace!("-> {:?}", buf);

                    let mut cur = Cursor::new(buf);

                    // Try to decode.
                    let val = match rmp::decode::read_value_ref(&mut cur) {
                        Ok(val) => val,
                        Err(err) => {
                            // TODO: Attach the proper error reason.
                            tx.send(Control::Shutdown(Error::DecodeError)).unwrap();
                            break;
                        }
                    };

                    let nread = cur.position() as usize;

                    // TODO: Replace with Display.
                    trace!("{} bytes is decoded as: {:?}", nread, val);

                    match Frame::new(val) {
                        Ok(frame) => {
                            tx.send(Control::Dispatch(frame)).unwrap();
                            nread
                        }
                        Err(err) => {
                            tx.send(Control::Shutdown(Error::FrameError(err))).unwrap();
                            break;
                        }
                    }
                }
                Err(err) => {
                    error!("-> error: {}", err);

                    tx.send(Control::Shutdown(Error::Io(err))).unwrap();
                    break;
                }
            };

            rd.consume(nread);
        }

        debug!("leave reader task");
    }

    fn dispatch(rx: mpsc::Receiver<Control>) {
        let mut channels = HashMap::new();

        // This loop will exhause when the last control sender dies, i.e when `Service` and all
        // `Receiver` instances are destroyed as like as the reader thread.
        for event in rx {
            debug!("processing {:?}", event);

            match event {
                Control::Invoke(id, tx) => {
                    channels.insert(id, tx);
                }
                Control::Revoke(id) => {
                    channels.remove(&id);
                }
                Control::Dispatch(frame) => {
                    match channels.get(&frame.id()) {
                        Some(tx) => {
                            // If we are here, then the User has dropped its receiver but the
                            // notification hasn't come yet (but it's pending!). The channel will
                            // be removed once the notification is delivered.
                            if tx.send(Ok(frame.into_payload())).is_err() {
                                warn!("unable to dispatch event: there is no receiver");
                            }
                        }
                        None => {
                            warn!("dropping unexpected frame with {} span", frame.id());
                        }
                    }
                }
                Control::Shutdown(err) => {
                    // If we are here, then the reader thread has been stopped. That means that no
                    // Dispatch events come.

                    for (id, tx) in channels.drain() {
                        warn!("closing channel with {} span: {:?}", id, err);

                        if tx.send(Err(err.clone())).is_err() {
                            warn!("unable to dispatch error event: there is no receiver");
                        }
                    }
                }
            }
        }

        debug!("leave dispatch task");
    }

    // TODO: Join method.
    // Consumes this service and waits until all outstanding asynchronous operations complete.
    //
    // Note, that calling this method can hang forever if there are living `Sender` or `Receiver`
    // instances somewhere.
    // fn join(self) {
    //     // Worker threads should be valid and should never panic.
    //     let threads = self.threads.take().unwrap();
    //
    //     // TODO: We easily can implement detach functionality, but in this case we shouldn't
    //     // shutdown the stream. Instead some kind of Arc should be both in Sender and Receiver.
    //     // When counter == 0 -> shutdown each side separately.
    //
    //     threads.0.join().unwrap();
    //     threads.1.join().unwrap();
    //     threads.2.join().unwrap();
    // }

    // TODO: Close method.
    // Closes both sides of the TCP transport.
    // fn close(&mut self).join(self);
}

// Worker side:
// 1. Register all events.
// 2. Run - tx + rx.
// -. Send handshake.
// -. Every n seconds send heartbeat and wait for response.
// 3. Loop in a separate thread and dispatch tasks.
// -. ChannelId == 1 -> Control | RPC.
// 4. 0 - heartbeat | Invoke | Write, etc.
// 5.
