#![feature(associated_type_defaults)]

extern crate cocaine;
extern crate futures;
extern crate rmpv;
#[macro_use]
extern crate slog;
extern crate slog_envlogger;
extern crate slog_stdlog;
extern crate slog_term;
extern crate tokio_core;

use futures::future::Future;
use futures::sync::oneshot;
use tokio_core::reactor::Core;

use slog::{DrainExt, Logger};

use rmpv::ValueRef;

use cocaine::{Dispatch, Error, Request, Service};
use cocaine::protocol::{self, Flatten, Primitive};

struct ReadDispatch {
    tx: oneshot::Sender<Result<String, Error>>,
}

impl Dispatch for ReadDispatch {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        let result = protocol::deserialize::<Primitive<String>>(ty, response).flatten();

        drop(self.tx.send(result));
        None
    }

    fn discard(self: Box<Self>, _err: &Error) {
        drop(self.tx);
    }
}

fn init() {
    let log = slog_term::streamer().compact().build().fuse();
    let log = slog_envlogger::new(log);
    let log = Logger::root(log, o!());
    slog_stdlog::set_logger(log).unwrap();
}

fn main() {
    init();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let service = Service::new("storage", &handle);

    let (tx, rx) = oneshot::channel();
    let future = service.call(Request::new(0, &["collection", "key"]).unwrap(), ReadDispatch { tx: tx })
        .then(|_sender| Ok(()));

    drop(service); // Just for fun to check that all pending request are proceeded until complete.
    handle.spawn(future);

    core.run(rx).unwrap().unwrap();
}
