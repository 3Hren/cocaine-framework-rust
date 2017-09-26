use serde::Deserialize;
use rmpv::{self, ValueRef};

use Error;
use protocol;

/// Immutable header view.
///
/// This struct represents a HPACK header received from a service. It is immutable, because the
/// data it points on lays directly in the socket buffer, which makes deserialization so fast.
#[derive(Debug, Deserialize, PartialEq)]
pub struct HeaderRef<'a> {
    cached: bool,
    name: &'a [u8],
    data: &'a [u8],
}

impl<'a> HeaderRef<'a> {
    /// Returns a header name.
    #[inline]
    pub fn name(&self) -> &[u8] {
        self.name
    }

    /// Returns a header data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data
    }
}

/// Generic response type.
#[derive(Debug)]
pub struct Response<'a: 'b, 'b> {
    ty: u64,
    args: &'b ValueRef<'a>,
    meta: Vec<HeaderRef<'b>>,
}

impl<'a: 'b, 'b> Response<'a, 'b> {
    pub(crate) fn new(ty: u64, args: &'b ValueRef<'a>, meta: &'b ValueRef<'a>) -> Result<Self, rmpv::ext::Error> {
        let resp = Self {
            ty: ty,
            args: args,
            meta: rmpv::ext::deserialize_from(meta)?,
        };

        Ok(resp)
    }

    /// Returns a response type.
    #[inline]
    pub fn ty(&self) -> u64 {
        self.ty
    }

    /// Deserializes the response into the specified type.
    ///
    /// Note, that this method can also be used to deserialize a response into some borrowed type,
    /// like `&str`, `&[u8]` or into an user-defined type that contains them. In this case the
    /// deserialization is completely zero-copy.
    ///
    /// # Errors
    ///
    /// This method can fail if the underlying deserializer decides to fail.
    ///
    /// # Examples
    ///
    /// This example demonstrates how to use deserialization into a borrowed struct.
    ///
    /// ```
    /// # #[macro_use] extern crate serde_derive;
    /// # extern crate cocaine;
    /// use cocaine::{Error, Response};
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct User<'a> {
    ///     name: &'a str,
    ///     age: u16,
    /// }
    ///
    /// fn handle(resp: &Response) -> Result<(), Error> {
    ///     let user: User = resp.deserialize()?;
    ///     println!("User: {:?}", user);
    ///     Ok(())
    /// }
    /// # fn main() {}
    /// ```
    #[inline]
    pub fn deserialize<T: Deserialize<'b>>(&self) -> Result<T, Error> {
        protocol::deserialize(self.ty, self.args)
    }

    /// Returns response headers.
    #[inline]
    pub fn headers(&self) -> &[HeaderRef] {
        &self.meta
    }
}
