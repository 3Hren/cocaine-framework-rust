extern crate cocaine;
extern crate tokio_core;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};

use tokio_core::reactor::Core;

use cocaine::connect;

#[test]
fn test_connection_refused() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let future = connect(vec![], &handle);

    assert!(core.run(future).is_err());
}

#[test]
fn test_connect() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let listener = TcpListener::bind(("0.0.0.0", 0)).unwrap();
    let addr = listener.local_addr().unwrap();

    let future = connect(vec![addr], &handle);

    assert_eq!(addr.port(), core.run(future).unwrap().peer_addr().unwrap().port());
}

#[test]
fn test_connect_multi() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let listener = TcpListener::bind(("0.0.0.0", 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let addrs = vec![
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 500)),
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 600)),
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 700)),
        addr,
    ];

    let future = connect(addrs, &handle);

    assert_eq!(addr.port(), core.run(future).unwrap().peer_addr().unwrap().port());
}
