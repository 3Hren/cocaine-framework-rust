#![feature(drain)]
#![feature(slice_patterns)]

#[macro_use] extern crate log;

extern crate rmp;
extern crate rmp_serde;
extern crate serde;

// For worker side.
extern crate argparse;
extern crate unix_socket;

pub mod raw;
pub mod worker;

mod protocol;
mod error;

pub use error::Error;
