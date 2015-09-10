#![feature(associated_type_defaults)]
#![feature(slice_patterns)]

#[macro_use] extern crate log;

extern crate mio;
extern crate mioco;
extern crate rmp;
extern crate rmp_serde;
extern crate serde;

use std::marker::PhantomData;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::net::{SocketAddr};

use serde::Serialize;

use mio::tcp::{TcpStream, Shutdown};

use mioco::{EventSource, MiocoHandle, MailboxOuterEnd, MailboxInnerEnd, mailbox};

use rmp::value::{Integer, ValueRef};
use rmp_serde::Serializer;

#[derive(Debug, Clone)]
pub struct Error;

// TODO: Must be Sync + Send.
// TODO: Auto reconnectable wrapper.

#[derive(Clone)]
enum Control {
    Invoke(u64, MailboxOuterEnd<Result<(u64, Vec<u8>), Error>>),
    Recv(u64, u64, Vec<u8>),
    Shutdown,
}

pub struct Service {
    /// TCP transport.
    stream: EventSource<TcpStream>,

    /// Dispatch sender.
    tx: MailboxOuterEnd<Control>,

    /// Channel id counter.
    counter: u64,
}

impl Service {
    pub fn connect(endpoint: &SocketAddr, mioco: &mut MiocoHandle) -> Result<Service, Error>
    {
        let stream = TcpStream::connect(&endpoint).unwrap();

        let (tx, rx) = mailbox();

        let service = Service {
            stream: mioco.wrap(stream.try_clone().unwrap()),
            tx: tx.clone(),
            counter: 0,
        };

        mioco.spawn(move |mioco| {
            let mut rx = mioco.wrap(rx);
            let mut channels = HashMap::new();

            loop {
                let event = rx.recv().unwrap();

                match event {
                    Control::Invoke(span, tx) => {
                        debug!("invoke: span={}", span);
                        channels.insert(span, tx);
                    }
                    Control::Recv(span, ty, data) => {
                        debug!("recv: span={}, type={}, data={:?}", span, ty, data);
                        match channels.get(&span) {
                            Some(tx) => {
                                tx.send(Ok((ty, data))).unwrap();
                            }
                            None => {
                                // Drop.
                            }
                        }
                    }
                    Control::Shutdown => {
                        break;
                    }
                }
            }

            debug!("leave dispatch task");

            Ok(())
        });

        mioco.spawn(move |mioco| {
            // let mut rd = BufReader::with_capacity(4096, mioco.wrap(stream));
            let mut rd = BufReader::new(mioco.wrap(stream));

            loop {
                let (span, ty, data, nread) = match rd.fill_buf() {
                    Ok([]) => {
                        debug!("read EOF");
                        break;
                    }
                    Ok(buf) => {
                        trace!("fill_buf, buf: {:?}", buf);
                        let mut cur = Cursor::new(buf);

                        // Try to decode.
                        let val = rmp::decode::read_value_ref(&mut cur).unwrap();
                        trace!("val: {:?}", val);

                        let (span, ty) = if let ValueRef::Array(val) = val {
                            if val.len() < 3 {
                                break;
                            }

                            let span = if let ValueRef::Integer(span) = val[0] {
                                if let Integer::U64(span) = span {
                                    span
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            };

                            let ty = if let ValueRef::Integer(ty) = val[1] {
                                if let Integer::U64(ty) = ty {
                                    ty
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            };

                            if let ValueRef::Array(..) = val[2] {
                                // Okay.
                            } else {
                                break;
                            }

                            (span, ty)
                        } else {
                            break;
                        };

                        let nread = cur.position() as usize;
                        trace!("nread: {}", nread);
                        (span, ty, cur.get_ref()[..nread].to_vec(), nread)
                    }
                    Err(err) => {
                        // Notify all channels.
                        error!("{:?}", err);
                        unimplemented!();
                    }
                };

                rd.consume(nread);

                tx.send(Control::Recv(span, ty, data)).unwrap();
            }

            debug!("leave reader task");

            Ok(())
        });

        Ok(service)
    }

    pub fn invoke<A>(&mut self, ty: u16, args: &A, mioco: &mut MiocoHandle) -> Result<(Tx, Rx), Error>
        where A: Serialize
    {
        self.counter += 1;

        // Serialize arguments.
        let mut buf = Vec::new();
        (self.counter, ty, args).serialize(&mut Serializer::new(&mut buf)).unwrap();
        trace!("invoking, buf: {:?}", buf);

        let (tx, rx) = mailbox();
        self.tx.send(Control::Invoke(self.counter, tx)).unwrap();

        // NOTE: On case of multithreaded scheduler we need to acquire lock here to prevent races.
        self.stream.write(&buf).unwrap();

        let stream = self.stream.with_raw_mut(|stream| {
            stream.try_clone().unwrap()
        });

        let tx = Tx {
            channel: self.counter,
            stream: mioco.wrap(stream),
        };

        Ok((tx, mioco.wrap(rx)))
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.tx.send(Control::Shutdown).unwrap();

        self.stream.with_raw_mut(|stream| {
            match stream.shutdown(Shutdown::Both) {
                Ok(()) => {}
                Err(..) => {}
            }
        });
    }
}

// fn connect<A>(addr: A) -> Result<(TcpStream, SocketAddr), Error>
//     where A: ToSocketAddrs
// {
//     let addrs = addr.to_socket_addrs().unwrap();
//
//     for addr in addrs {
//         debug!("connecting to {} ...", addr);
//         match TcpStream::connect(&addr) {
//             Ok(stream) => {
//                 return Ok((stream, addr));
//             }
//             Err(err) => {
//                 warn!("failed to connect: {}", err);
//                 continue;
//             }
//         }
//     }
//
//     // TODO: Something wrong happens when connection actually refused.
//     // TODO: Connection refused.
//     Err(Error)
// }

pub struct Channel;

pub struct Tx {
    channel: u64,
    stream: EventSource<TcpStream>,
}

impl Tx {
    pub fn send<T>(&mut self, ty: u16, args: &T) -> Result<(), Error>
        where T: Serialize
    {
        let mut buf = Vec::new();
        (self.channel, ty, args).serialize(&mut Serializer::new(&mut buf)).unwrap();
        trace!("push: {:?}", buf);

        self.stream.write(&buf).unwrap();

        Ok(())
    }
}

pub type Rx = EventSource<MailboxInnerEnd<Result<(u64, Vec<u8>), Error>>>;
