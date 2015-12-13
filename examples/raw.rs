#![feature(ip_addr)]

extern crate cocaine;

use std::net::{SocketAddr, IpAddr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use cocaine::raw::Service;

fn with_threads(concurrency: u32) {
    let endpoint = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 10054);

    // let service = Arc::new(Mutex::new(Service::connect(&endpoint).unwrap()));
    let mut threads = Vec::new();
    let n = Arc::new(AtomicUsize::new(0));
    let max = 100000;

    for tid in 0..concurrency {
        let n = n.clone();
        // let service = service.clone();
        let service = Arc::new(Mutex::new(Service::connect(&endpoint).unwrap()));

        let th = thread::spawn(move || {
            let mut counter = 0;

            loop {
                if n.fetch_add(1, Ordering::SeqCst) >= max {
                    break;
                }

                let (tx, rx) = {
                    let mut service = service.lock().unwrap();
                    service.invoke(0, &["ping"]).unwrap()
                };

                tx.send(0, &["le message"]).unwrap();
                tx.send(2, &[0u8; 0]).unwrap();

                // Write.
                let (ty, _) = rx.recv().unwrap();
                assert_eq!(0, ty);

                // Close.
                let (ty, _) = rx.recv().unwrap();
                assert_eq!(2, ty);

                let i = n.load(Ordering::Relaxed);
                if i % 1000 == 0 {
                    println!("i: {}", i);
                }

                counter += 1;
            }

            println!("{}: {}", tid, counter);
        });

        threads.push(th);
    }

    for th in threads {
        th.join().unwrap();
    }
}

fn main() {
    with_threads(64);
}
