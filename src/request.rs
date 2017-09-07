use std::fmt::{self, Debug, Formatter};

use serde::Serialize;
use rmps;

use hpack::RawHeader;

#[derive(Clone)]
pub struct Request {
    ty: u64,
    buf: Vec<u8>,
    headers: Vec<RawHeader>,
}

impl Request {
    #[inline]
    pub fn new<S: Serialize>(ty: u64, args: &S) -> Result<Self, rmps::encode::Error> {
        let buf = rmps::to_vec(args)?;
        let request = Request::from_buf(ty, buf);

        Ok(request)
    }

    #[inline]
    pub fn ty(&self) -> u64 {
        self.ty
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn headers(&self) -> &[RawHeader] {
        &self.headers
    }

    pub fn add_header<H: Into<RawHeader>>(mut self, header: H) -> Self {
        self.headers.push(header.into());
        self
    }

    pub fn add_headers<H: IntoIterator<Item=RawHeader>>(mut self, headers: H) -> Self {
        self.headers.extend(headers);
        self
    }

    pub(crate) fn into_components(self) -> (Vec<u8>, Vec<RawHeader>) {
        (self.buf, self.headers)
    }

    #[inline]
    pub(crate) fn from_buf(ty: u64, buf: Vec<u8>) -> Self {
        Self {
            ty: ty,
            buf: buf,
            headers: Vec::new(),
        }
    }
}

impl Debug for Request {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Request")
            .field("ty", &self.ty)
            .field("len", &self.buf.len())
            .field("headers", &self.headers)
            .finish()
    }
}
