extern crate cocaine;
extern crate futures;
extern crate libc;
extern crate net2;
extern crate rmp_serde as rmps;
extern crate rmpv;
extern crate tokio_core;

use std::io::{ErrorKind, Write};
use std::net::{TcpListener, SocketAddr, IpAddr, Ipv4Addr};
use std::os::unix::io::IntoRawFd;
use std::thread;

use futures::sync::oneshot;
use net2::TcpStreamExt;
use rmpv::ValueRef;
use tokio_core::reactor::Core;

use cocaine::{Builder, Dispatch, Error, FixedResolver};

fn endpoint() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0)
}

#[test]
fn dispatch_receives_rst() {
    let sock = TcpListener::bind(&endpoint()).unwrap();
    let addr = sock.local_addr().unwrap();

    let thread = thread::spawn(move || {
        let (mut sock, ..) = sock.accept().unwrap();
        sock.set_linger(None).unwrap();

        let frame = rmps::to_vec(&(1, 0, &[0u8; 0])).unwrap();
        sock.write(&frame[..2]).unwrap();

        // Close the socket, forcing it to send RST.
        unsafe {
            libc::close(sock.into_raw_fd());
        }
    });

    let mut core = Core::new().unwrap();

    let service = Builder::new("service")
        .resolver(FixedResolver::new(vec![addr]))
        .build(&core.handle());

    struct MockDispatch {
        tx: oneshot::Sender<()>,
    }

    impl Dispatch for MockDispatch {
        fn process(self: Box<Self>, _ty: u64, _response: &ValueRef) -> Option<Box<Dispatch>> {
            panic!("expected calling `discard`, called `process`");
        }

        fn discard(self: Box<Self>, err: &Error) {
            match err {
                &Error::Io(ref err) => {
                    assert!(ErrorKind::ConnectionReset == err.kind() ||
                            ErrorKind::UnexpectedEof == err.kind());
                    drop(self.tx.send(()));
                }
                err => panic!("expected I/O error, actual {:?}", err),
            }
        }
    }

    let (tx, rx) = oneshot::channel();

    core.run(service.call(0, &["node"], MockDispatch { tx: tx })).unwrap();
    core.run(rx).unwrap();

    thread.join().unwrap();
}
