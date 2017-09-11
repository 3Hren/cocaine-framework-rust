extern crate cocaine;
extern crate futures;

use cocaine::{Core, Service};
use cocaine::service::Storage;

fn main() {
    let mut core = Core::new().unwrap();
    let storage = Storage::new(Service::new("storage", &core.handle()));

    let future = storage.write("collection", "key", "le message".as_bytes(), &[]);

    core.run(future).unwrap();
}
