extern crate cocaine;

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use cocaine::raw::Service;

fn main() {
    let ip = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1);
    let endpoint = SocketAddr::V6(SocketAddrV6::new(ip, 20000, 0, 0));

    let mut service = Service::connect(&endpoint).unwrap();

    let (tx, rx) = service.invoke(0, &["ping"]).unwrap();

    tx.send(0, &["le message"]).unwrap();
    tx.send(2, &[0u8; 0]).unwrap();

    // Write.
    let (ty, _) = rx.recv().unwrap();
    assert_eq!(0, ty);

    // Close.
    let (ty, _) = rx.recv().unwrap();
    assert_eq!(2, ty);
}
