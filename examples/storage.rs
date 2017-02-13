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

use cocaine::{Dispatch, Service};

struct ReadDispatch {
    tx: oneshot::Sender<()>,
}

impl Dispatch for ReadDispatch {
    fn process(self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        self.tx.complete(());
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

    core.run(rx).unwrap();

    loop {
        core.turn(None);
    }
}

// s.call(Enqueue("ping"));
// // Sender<E::Next> == Sender<Streaming>
// // Receiver<E::Dispatch>
//
// #[derive(Deserialize)]
// enum StreamingEnum {
//     Write(Write),
//     Error(Error),
//     Close(Close),
// }
//
// struct App;
// struct Streaming;
//
// trait State {
//     type Dispatch: Deserialize = ();
// }
//
// impl State for () {}
// impl State for App {}
// impl State for Streaming {
//     type Dispatch = StreamingEnum;
// }
//
// struct Enqueue {
//     name: String,
// }
//
// struct Info;
//
// trait Event: Serialize {
//     type Scope: State = ();
// }
//
// impl Event for Enqueue {
//     type Scope = App;
// }
//
// impl Event for Info {}
//
// trait Transition {
//     type Next: State = ();
// }
//
// impl Transition for Enqueue {
//     type Next = Streaming;
// }
//
// impl Transition for Info {
//     type Next = ();
// }
//
// struct Write(String);
// struct Error(u64, u64, Option<String>);
// struct Close;
//
// impl Transition for Write {
//     type Next = Streaming;
// }
//
// impl Transition for Error {}
//
// impl Transition for Close {}
