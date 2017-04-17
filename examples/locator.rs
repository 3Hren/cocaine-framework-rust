extern crate cocaine;
extern crate futures;
extern crate tokio_core;
extern crate rmpv;

use futures::{future, Stream};
use tokio_core::reactor::Core;

use cocaine::{Service};
use cocaine::service::Locator;

fn main() {
    let mut core = Core::new().unwrap();
    let locator = Locator::new(Service::new("locator", &core.handle()));

    let stream = locator.routing("whatever").for_each(|chunk| {
        println!("{:?}", chunk);
        future::ok(())
    });

    core.run(stream).unwrap();
}