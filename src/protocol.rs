use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Deserializer};
use serde::de::Visitor;

use rmpv::{Value, ValueRef};
use rmpv::ext::{EnumDeserializer};

/// The error type which is returned from a cocaine service.
#[derive(Debug, Deserialize, Clone)]
pub struct Error {
    error: (u64, u64),
    description: Option<String>,
}

impl Error {
    pub fn new(category: u64, code: u64, description: Option<String>) -> Self {
        Self {
            error: (category, code),
            description: description,
        }
    }
    /// Returns error category number.
    pub fn category(&self) -> u64 {
        self.error.0
    }

    /// Returns error code number.
    pub fn code(&self) -> u64 {
        self.error.1
    }

    /// Returns optional error description, if provided.
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }

    /// Converts this error into an optional error description, if provided.
    pub fn into_description(self) -> Option<String> {
        self.description
    }
}

impl Display for Error {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        let desc = self.description.as_ref().map(|s| s.as_str()).unwrap_or("no description");

        write!(fmt, "[{}]: {}", self.code(), desc)
    }
}

impl Into<super::Error> for Error {
    fn into(self) -> super::Error {
        super::Error::Service(self)
    }
}

/// Primitive result, i.e. result, which can be only either a value or an error.
///
/// The majority of services uses this protocol for a single-shot responses.
///
/// # Note
///
/// Used primarily in conjunction with [`deserialize`][deserialize] function inside dispatches to
/// map arguments into the user-defined type.
///
/// [deserialize]: fn.deserialize.html
pub type Primitive<T> = Result<T, Error>;

/// Streaming result, i.e stream of some chunks that ends with either `Close` on normal execution
/// path or `Error` otherwise.
///
/// This is mainly an application protocol - all cocaine applications on `enqueue` method open a
/// stream of `Streaming<String>` type.
///
/// # Note
///
/// Used primarily in conjunction with [`deserialize`][deserialize] function inside dispatches to
/// map arguments into the user-defined type.
///
/// [deserialize]: fn.deserialize.html
///
#[derive(Debug, Deserialize)]
pub enum Streaming<T> {
    Write(T),
    Error(Error),
    Close,
}

/// An extension trait for results, that contain error types itself, to be able to flatten the
/// result of deserialization and protocol errors.
///
/// For example when deserializing a `Primitive` result either deserialization or service error can
/// occur, which results in `Result<Result<T, protocol::Error>, Error>` type. To avoid explicit
/// double matching the inner result can be flatten into the outer one.
pub trait Flatten {
    /// The success value type.
    type Item;

    /// Flattens the inner protocol, possibly containing an error type into the common result type.
    fn flatten(self) -> Result<Self::Item, super::Error>;
}

impl<T> Flatten for Result<Result<T, Error>, super::Error> {
    type Item = T;

    fn flatten(self) -> Result<Self::Item, super::Error> {
        match self {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(err)) => Err(err.into()),
            Err(err) => Err(err),
        }
    }
}

impl<T> Flatten for Result<Streaming<T>, super::Error> {
    type Item = Option<T>;

    fn flatten(self) -> Result<Self::Item, super::Error> {
        match self {
            Ok(Streaming::Write(v)) => Ok(Some(v)),
            Ok(Streaming::Error(e)) => Err(e.into()),
            Ok(Streaming::Close) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

struct PackedValue<'a>(u64, &'a ValueRef<'a>);

impl<'a> PackedValue<'a> {
    fn ty(&self) -> u64 {
        self.0
    }

    fn to_value(&self) -> Value {
        self.1.to_owned()
    }
}

impl<'a> Deserializer for PackedValue<'a> {
    type Error = super::Error;

    #[inline]
    fn deserialize<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor
    {
        unimplemented!();
    }

    #[inline]
    fn deserialize_enum<V>(self, _name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor
    {
        if self.ty() < variants.len() as u64 {
            let de = EnumDeserializer::new(self.ty() as u32, Some(self.to_value()));
            visitor.visit_enum(de)
                .map_err(|err| super::Error::InvalidDataFraming(format!("{}", err)))
        } else {
            Err(super::Error::InvalidDataFraming(format!("unexpected message with type {}", self.ty())))
        }
    }

    forward_to_deserialize! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit
        tuple seq seq_fixed_size bytes byte_buf map option
        unit_struct tuple_struct struct newtype_struct struct_field ignored_any
    }
}

/// Deserializes the dispatch arguments into some user-defined type `D`.
pub fn deserialize<D: Deserialize>(ty: u64, args: &ValueRef) -> Result<D, super::Error> {
    Deserialize::deserialize(PackedValue(ty, args))
}
