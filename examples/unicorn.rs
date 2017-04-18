extern crate cocaine;
extern crate futures;
extern crate tokio_core;

use futures::{Future, Stream};
use tokio_core::reactor::Core;

use cocaine::Service;
use cocaine::service::Unicorn;

fn main() {
    let mut core = Core::new().unwrap();
    let unicorn = Unicorn::new(Service::new("unicorn", &core.handle()));

    let future = unicorn.children_subscribe("/acl").and_then(|(tx, stream)| {
        stream.into_future().then(|r| {
            match r {
                Ok((nodes, stream)) => {
                    println!("nodes: {:?}", nodes);
                    drop(tx);
                    drop(stream);
                }
                Err(..) => {}
            }
            Ok(())
        })
    });
    core.run(future).unwrap();
}
