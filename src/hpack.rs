//! Header Compression for HTTP/2.

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq)]
pub struct Header {
    pub name: Cow<'static, [u8]>,
    pub data: Cow<'static, [u8]>,
}

impl Header {
    pub fn new<N, V>(name: N, data: V) -> Self
        where N: Into<Cow<'static, [u8]>>,
              V: Into<Cow<'static, [u8]>>,
    {
        Self {
            name: name.into(),
            data: data.into(),
        }
    }
}
