//! Header Compression for HTTP/2.

use std::borrow::Cow;

use byteorder::{ByteOrder, LittleEndian};

/// Raw HPACK header.
#[derive(Clone, Debug, PartialEq)]
pub struct RawHeader {
    /// Header name.
    pub name: Cow<'static, [u8]>,
    /// Header value.
    pub data: Cow<'static, [u8]>,
}

impl RawHeader {
    /// Constructs a raw HPACK header using the specified name and value.
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

/// A well-known predefined header.
pub trait Header {
    /// Returns a header name.
    fn name() -> &'static [u8];
    /// Returns a header value.
    fn data(&self) -> Cow<'static, [u8]>;

    /// Converts this header into a raw representation.
    fn into_raw(self) -> RawHeader where Self: Sized {
        RawHeader::new(Self::name(), self.data())
    }
}

fn pack_u64(v: u64) -> Vec<u8> {
    let mut buf = vec![0; 8];
    LittleEndian::write_u64(&mut buf[..], v);
    buf
}

/// Header for an unique request identifier.
///
/// Represents a trace id - a number, which identifies the request.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceId(pub u64);

impl Header for TraceId {
    fn name() -> &'static [u8] {
        b"trace_id"
    }

    fn data(&self) -> Cow<'static, [u8]> {
        match *self {
            TraceId(v) => pack_u64(v).into(),
        }
    }
}

/// Header for an unique sub-request identifier.
///
/// Represents a span id - a number, which identifies the sub-request.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanId(pub u64);

impl Header for SpanId {
    fn name() -> &'static [u8] {
        b"span_id"
    }

    fn data(&self) -> Cow<'static, [u8]> {
        match *self {
            SpanId(v) => pack_u64(v).into(),
        }
    }
}

/// Header for identifying a parent of the current span.
#[derive(Clone, Debug, PartialEq)]
pub struct ParentId(pub u64);

impl Header for ParentId {
    fn name() -> &'static [u8] {
        b"parent_id"
    }

    fn data(&self) -> Cow<'static, [u8]> {
        match *self {
            ParentId(v) => pack_u64(v).into(),
        }
    }
}

/// A header which determines whether the entire traced path should be logged verbosely.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceBit(pub bool);

impl Header for TraceBit {
    fn name() -> &'static [u8] {
        b"trace_bit"
    }

    fn data(&self) -> Cow<'static, [u8]> {
        if let TraceBit(true) = *self {
            b"1"[..].into()
        } else {
            b"0"[..].into()
        }
    }
}
