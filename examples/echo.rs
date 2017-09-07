extern crate cocaine;
extern crate futures;
#[macro_use]
extern crate slog;
extern crate slog_envlogger;
extern crate slog_stdlog;
extern crate slog_term;
extern crate tokio_core;

use futures::{Future, Stream, future};
use tokio_core::reactor::Core;

use slog::{DrainExt, Logger};

use cocaine::Service;
use cocaine::service::App;

fn init() {
    let log = slog_term::streamer().compact().build().fuse();
    let log = slog_envlogger::new(log);
    let log = Logger::root(log, o!());
    slog_stdlog::set_logger(log).unwrap();
}

fn main() {
    init();

    let mut core = Core::new().unwrap();
    let app = App::new(Service::new("echo-cpp", &core.handle()));

    let future = app.enqueue("ping").then(|r| {
        let (tx, rx) = r.unwrap();
        tx.write("ping");

        rx.for_each(|chunk| {
            println!("{:?}", chunk);
            future::ok(())
        })
    });

    drop(app); // Just for fun.
    core.run(future).unwrap();
}
