use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq)]
pub struct Header {
    pub name: Cow<'static, [u8]>,
    pub data: Cow<'static, [u8]>,
}
