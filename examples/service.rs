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

use slog::{Logger, DrainExt};

use rmpv::ValueRef;

use cocaine::{Dispatch, Error, Service};
use cocaine::protocol::{self, Flatten, Primitive};

struct ReadDispatch {
    tx: oneshot::Sender<Result<String, Error>>,
}

impl Dispatch for ReadDispatch {
    fn process(self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        let result = protocol::deserialize::<Primitive<String>>(ty, response)
            .flatten();

        drop(self.tx.send(result));
        None
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
    let future = service.call(0, &vec!["collection", "key"], ReadDispatch { tx: tx })
        .and_then(|_tx| Ok(()))
        .then(|_result| Ok(()));

    drop(service); // Just for fun.
    handle.spawn(future);

    let result = core.run(rx).unwrap();
    println!("{:?}", result);
}
