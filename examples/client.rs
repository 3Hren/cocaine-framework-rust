#![feature(ip_addr)]

extern crate cocaine;

use std::net::{SocketAddr, IpAddr, Ipv6Addr};

use cocaine::raw::Service;

fn main() {
    let endpoint = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 20000);

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
