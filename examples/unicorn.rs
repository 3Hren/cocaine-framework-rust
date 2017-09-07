extern crate cocaine;
extern crate futures;
#[macro_use]
extern crate serde_derive;

use futures::{Future, Stream};

use cocaine::{Core, Service};
use cocaine::service::Unicorn;

#[derive(Debug, Deserialize)]
struct Resource {
    cpu: i64,
    mem: i64,
    net: i64,
}

fn main() {
    let mut core = Core::new().unwrap();
    let unicorn = Unicorn::new(Service::new("unicorn", &core.handle()));

    let future = unicorn.children_subscribe("/darkvoice/resources".into()).and_then(|(tx, stream)| {
        stream.take(1).for_each(|(version, nodes)| {
            println!("Version: {}, nodes: {:?}", version, nodes);

            let mut futures = Vec::with_capacity(5001);
            for node in nodes {
                futures.push(unicorn.get::<Resource>(&format!("/darkvoice/resources/{}", node)));
            }

            futures::future::join_all(futures).and_then(|result| {
                println!("Result: {:?}", result);
                Ok(())
            })
        }).and_then(|()| {
            drop(tx);
            Ok(())
        })
    });
    core.run(future).unwrap();
}
