#![feature(proc_macro, conservative_impl_trait, generators)]

extern crate cocaine;
extern crate futures_await as futures;
#[macro_use]
extern crate serde_derive;

use futures::prelude::*;

use cocaine::{Core, Error, Service};
use cocaine::service::Unicorn;

#[derive(Debug, Deserialize)]
struct Resource {
    cpu: i64,
    mem: i64,
    net: i64,
}

#[async]
fn run(unicorn: Unicorn) -> Result<(), Error> {
    let (tx, stream) = await!(unicorn.children_subscribe("/darkvoice/resources".into()))?;

    #[async]
    for (version, nodes) in stream {
        println!("Version: {}, nodes: {:?}", version, nodes);

        for node in nodes {
            let (resources, version) = await!(unicorn.get::<Resource>(&format!("/darkvoice/resources/{}", node)))?;
            println!("Resources for {}: {:?}; version={}", node, resources, version);
        }
    }

    tx.close();
    Ok(())
}

fn main() {
    let mut core = Core::new().unwrap();
    let unicorn = Unicorn::new(Service::new("unicorn", &core.handle()));
    core.run(run(unicorn)).unwrap();
}
