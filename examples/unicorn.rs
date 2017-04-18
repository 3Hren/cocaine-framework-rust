extern crate cocaine;
extern crate futures;

use futures::{Future, Stream};

use cocaine::{Core, Service};
use cocaine::service::Unicorn;

fn main() {
    let mut core = Core::new().unwrap();
    let unicorn = Unicorn::new(Service::new("unicorn", &core.handle()));

    let future = unicorn.children_subscribe("/acl").and_then(|(tx, stream)| {
        stream.take(1).for_each(|nodes| {
            println!("nodes: {:?}", nodes);
            Ok(())
        }).and_then(|()| {
            drop(tx);
            Ok(())
        })
    });
    core.run(future).unwrap();
}
