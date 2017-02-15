use std::fmt::{self, Display, Formatter};
use rmpv::ValueRef;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Error {
    TypeMismatch,
    SizeMismatch(usize),
}

impl Display for Error {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::TypeMismatch => write!(fmt, "type mismatch"),
            Error::SizeMismatch(size) => write!(fmt, "size mismatch, expected 3 or 4, found {}", size),
        }
    }
}

static NIL: ValueRef<'static> = ValueRef::Nil;

#[derive(Debug)]
pub struct Frame<'a: 'b, 'b> {
    id: u64,
    ty: u64,
    args: &'b ValueRef<'a>,
    meta: &'b ValueRef<'a>,
}

impl<'a, 'b> Display for Frame<'a, 'b> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        write!(fmt, "[{}, {}, {}, [{}]]", self.id, self.ty, self.args, self.meta)
    }
}

impl<'a, 'b> Frame<'a, 'b> {
    pub fn new(frame: &'b ValueRef<'a>) -> Result<Self, Error> {
        if let &ValueRef::Array(ref vec) = frame {
            match vec.len() {
                3 => {
                    let id = vec[0].as_u64().ok_or(Error::TypeMismatch)?;
                    let ty = vec[1].as_u64().ok_or(Error::TypeMismatch)?;
                    let args = &vec[2];

                    let message = Frame {
                        id: id,
                        ty: ty,
                        args: args,
                        meta: &NIL,
                    };

                    Ok(message)
                }
                4 => {
                    let id = vec[0].as_u64().ok_or(Error::TypeMismatch)?;
                    let ty = vec[1].as_u64().ok_or(Error::TypeMismatch)?;
                    let args = &vec[2];
                    let meta = &vec[3];

                    let message = Frame {
                        id: id,
                        ty: ty,
                        args: args,
                        meta: meta,
                    };

                    Ok(message)
                }
                len => Err(Error::SizeMismatch(len)),
            }
        } else {
            Err(Error::TypeMismatch)
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn ty(&self) -> u64 {
        self.ty
    }

    pub fn args(&self) -> &ValueRef {
        self.args
    }
}
