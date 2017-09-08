//! Request build and management.

use std::fmt::{self, Debug, Formatter};

use serde::Serialize;
use rmps;

use hpack::RawHeader;

/// A generic Cocaine request.
///
/// Encapsulates all required parameters to be able to perform a service call.
///
/// # Examples
///
/// ```
/// use cocaine::Request;
/// use cocaine::hpack::{Header, TraceId, SpanId};
///
/// let request = Request::new(0, &["event"]).unwrap()
///     .add_headers(vec![TraceId(0).into_raw(), SpanId(42).into_raw()]);
///
/// assert_eq!(0, request.ty());
/// ```
///
/// # Errors
///
/// A serialization error is returned when it's failed to serialize the given arguments into a
/// vector. However, while a vector is grown automatically, it may only fail when there is no
/// memory left for allocation.
#[derive(Clone)]
pub struct Request {
    ty: u64,
    buf: Vec<u8>,
    headers: Vec<RawHeader>,
}

impl Request {
    /// Constructs a new request object using the given message type and arguments, performing an
    /// automatic serialization.
    #[inline]
    pub fn new<S: Serialize>(ty: u64, args: &S) -> Result<Self, rmps::encode::Error> {
        let buf = rmps::to_vec(args)?;
        let request = Request::from_buf(ty, buf);

        Ok(request)
    }

    /// Returns a request type.
    #[inline]
    pub fn ty(&self) -> u64 {
        self.ty
    }

    /// Returns a serialized request arguments as a slice.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.buf
    }

    /// Returns a reference to the request headers.
    #[inline]
    pub fn headers(&self) -> &[RawHeader] {
        &self.headers
    }

    /// Adds a header to the request.
    pub fn add_header<H: Into<RawHeader>>(mut self, header: H) -> Self {
        self.headers.push(header.into());
        self
    }

    /// Adds an iterable headers object to the request.
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
