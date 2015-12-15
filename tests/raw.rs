extern crate cocaine;
extern crate env_logger;
extern crate rmp;

use std::io::{Read, Write};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6, TcpListener};
use std::sync::mpsc;
use std::thread;

use rmp::Value;

use cocaine::Error;
use cocaine::raw::Service;

#[test]
fn fail_connect() {
    let ip = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1);
    let endpoint = SocketAddr::V6(SocketAddrV6::new(ip, 0, 0, 0));
    let err = Service::connect(&endpoint).err().unwrap();

    match err {
        Error::Io(..) => {}
        other => panic!("expected I/O error, found: {:?}", other),
    }
}

#[test]
fn pass_connect() {
    env_logger::init().unwrap();

    let (tx, rx) = mpsc::channel();

    let thread = thread::spawn(move || {
        let acceptor = TcpListener::bind(("::", 0)).unwrap();
        let endpoint = acceptor.local_addr().unwrap();
        tx.send(endpoint).unwrap();

        acceptor.accept().ok().expect("expected socket acception");
    });

    let endpoint = rx.recv().unwrap();

    Service::connect(&endpoint).unwrap();

    thread.join().unwrap();
}

#[test]
fn pass_invoke_primitive() {
    let (tx, rx) = mpsc::channel();

    let thread = thread::spawn(move || {
        let acceptor = TcpListener::bind(("::", 0)).unwrap();
        let endpoint = acceptor.local_addr().unwrap();
        tx.send(endpoint).unwrap();

        let mut stream = acceptor.accept().ok().expect("expected socket acception").0;

        // Wait for properly encoded frame.
        let mut buf = vec![0; 12];

        // The frame is: [1, 0, ['storage']].
        let expected = vec![0x93, 0x1, 0x0, 0x91, 0xa7, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65];

        stream.read_exact(&mut buf).unwrap();
        // The frame is: [1, 0, []].
        stream.write_all(&[0x93, 0x1, 0x0, 0x90]).unwrap();

        drop(stream);

        assert_eq!(expected, buf);
    });

    let endpoint = rx.recv().unwrap();

    let mut service = Service::connect(&endpoint).unwrap();
    let (_, rx) = service.invoke(0, &["storage"]).unwrap();
    let (ty, data) = rx.recv().unwrap();

    assert_eq!(0, ty);
    assert_eq!(Value::Array(vec![]), data);

    thread.join().unwrap();
}

#[test]
fn fail_reset_connection_after_connected() {
    // We expect to receive ECONNRESET while reading. We also MAY receive EPIPE while writing.

    let (tx, rx) = mpsc::channel();

    let thread = thread::spawn(move || {
        let acceptor = TcpListener::bind(("::", 0)).unwrap();
        let endpoint = acceptor.local_addr().unwrap();
        tx.send(endpoint).unwrap();

        acceptor.accept().ok().expect("expected socket acception");
    });

    let endpoint = rx.recv().unwrap();

    let mut service = Service::connect(&endpoint).unwrap();
    let (_, rx) = service.invoke(0, &["storage"]).unwrap();
    match rx.recv() {
        Err(Error::Io(..)) => {},
        other => panic!("unexpected result: {:?}", other),
    }

    thread.join().unwrap();
}

#[test]
fn fail_invoke_return_decode_error() {
    let (tx, rx) = mpsc::channel();

    let thread = thread::spawn(move || {
        let acceptor = TcpListener::bind(("::", 0)).unwrap();
        let endpoint = acceptor.local_addr().unwrap();
        tx.send(endpoint).unwrap();

        let mut stream = acceptor.accept().ok().expect("expected socket acception").0;

        // Wait for properly encoded frame.
        let mut buf = vec![0; 12];

        // The frame is: [1, 0, ['storage']].
        let expected = vec![0x93, 0x1, 0x0, 0x91, 0xa7, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65];

        stream.read_exact(&mut buf).unwrap();
        // The frame is malformed: [1, 0, ?].
        stream.write_all(&[0x93, 0x1, 0x0]).unwrap();

        drop(stream);

        assert_eq!(expected, buf);
    });

    let endpoint = rx.recv().unwrap();

    let mut service = Service::connect(&endpoint).unwrap();
    let (_, rx) = service.invoke(0, &["storage"]).unwrap();
    match rx.recv() {
        Err(Error::DecodeError) => {},
        other => panic!("unexpected result: {:?}", other),
    }

    thread.join().unwrap();
}

#[test]
fn fail_invoke_return_invalid_protocol() {
    let (tx, rx) = mpsc::channel();

    let thread = thread::spawn(move || {
        let acceptor = TcpListener::bind(("::", 0)).unwrap();
        let endpoint = acceptor.local_addr().unwrap();
        tx.send(endpoint).unwrap();

        let mut stream = acceptor.accept().ok().expect("expected socket acception").0;

        // Wait for properly encoded frame.
        let mut buf = vec![0; 12];

        // The frame is: [1, 0, ['storage']].
        let expected = vec![0x93, 0x1, 0x0, 0x91, 0xa7, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65];

        stream.read_exact(&mut buf).unwrap();
        // The frame is malformed: [1, 0].
        stream.write_all(&[0x92, 0x1, 0x0]).unwrap();

        drop(stream);

        assert_eq!(expected, buf);
    });

    let endpoint = rx.recv().unwrap();

    let mut service = Service::connect(&endpoint).unwrap();
    let (_, rx) = service.invoke(0, &["storage"]).unwrap();
    match rx.recv() {
        Err(Error::FrameError(..)) => {},
        other => panic!("unexpected result: {:?}", other),
    }

    thread.join().unwrap();
}

#[test]
fn pass_invoke_streaming() {
    let (tx, rx) = mpsc::channel();

    let thread = thread::spawn(move || {
        let acceptor = TcpListener::bind(("::", 0)).unwrap();
        let endpoint = acceptor.local_addr().unwrap();
        tx.send(endpoint).unwrap();

        let mut stream = acceptor.accept().ok().expect("expected socket acception").0;

        // Wait for properly encoded frame.

        // The frame is: [1, 0, ['ping']].
        let expected = vec![0x93, 0x1, 0x0, 0x91, 0xa4, 0x70, 0x69, 0x6e, 0x67];
        let mut buf = vec![0; 9];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(expected, buf);

        // The frame is: [1, 0, ['le message']].
        let expected = vec![0x93, 0x1, 0x0, 0x91, 0xaa, 0x6c, 0x65, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65];
        let mut buf = vec![0; 15];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(expected, buf);

        // The frame is: [1, 2, ['']].
        let expected = vec![0x93, 0x1, 0x2, 0x90];
        let mut buf = vec![0; 4];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(expected, buf);

        // The frames are: [1, 0, ['le message']]; [1, 2, []].
        stream.write_all(&vec![0x93, 0x1, 0x0, 0x91, 0xaa, 0x6c, 0x65, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65]).unwrap();
        stream.write_all(&[0x93, 0x1, 0x2, 0x90]).unwrap();

        drop(stream);
    });

    let endpoint = rx.recv().unwrap();

    let mut service = Service::connect(&endpoint).unwrap();
    let (tx, rx) = service.invoke(0, &["ping"]).unwrap();
    tx.send(0, &["le message"]).unwrap();
    tx.send(2, &[0u8; 0]).unwrap();

    // Write.
    let (ty, data) = rx.recv().unwrap();
    assert_eq!(0, ty);
    assert_eq!(Value::Array(vec![Value::String("le message".to_owned())]), data);

    // Close.
    let (ty, data) = rx.recv().unwrap();
    assert_eq!(2, ty);
    assert_eq!(Value::Array(vec![]), data);

    thread.join().unwrap();
}

// TODO: Test that channels are monotonically increased.
