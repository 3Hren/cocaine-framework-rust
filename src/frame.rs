use std::error;
use std::fmt::{self, Display, Formatter};
use rmpv::ValueRef;

/// Framing error.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Error {
    /// Frame must be an array.
    InvalidFrameType,
    /// Frame size must be 3 or 4.
    SizeMismatch(usize),
    /// Channel id type must be an u64.
    SpanTypeMismatch,
    /// Type id type must be an u64.
    TypeTypeMismatch,
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::InvalidFrameType => "protocol frame has invalid type",
            Error::SizeMismatch(..) => "protocol frame has invalid size, expected 3 or 4",
            Error::SpanTypeMismatch => "span field has invalid type",
            Error::TypeTypeMismatch => "type field has invalid type",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

impl Display for Error {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::InvalidFrameType => "protocol frame has invalid type, must be an array".fmt(fmt),
            Error::SizeMismatch(size) => {
                write!(fmt, "protocol frame has invalid size, expected 3 or 4, found {}", size)
            }
            Error::SpanTypeMismatch => "span field has invalid type".fmt(fmt),
            Error::TypeTypeMismatch => "type field has invalid type".fmt(fmt),
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
        if let ValueRef::Array(ref vec) = *frame {
            match vec.len() {
                3 => {
                    let id = vec[0].as_u64().ok_or(Error::SpanTypeMismatch)?;
                    let ty = vec[1].as_u64().ok_or(Error::TypeTypeMismatch)?;
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
                    let id = vec[0].as_u64().ok_or(Error::SpanTypeMismatch)?;
                    let ty = vec[1].as_u64().ok_or(Error::TypeTypeMismatch)?;
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
            Err(Error::InvalidFrameType)
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

#[cfg(test)]
mod test {
    use rmpv::ValueRef;

    use super::Frame;

    #[test]
    fn pass() {
        let val = ValueRef::Array(vec![
            ValueRef::from(1),
            ValueRef::from(0),
            ValueRef::Array(vec![]),
        ]);

        let frame = Frame::new(&val).unwrap();
        assert_eq!(1, frame.id());
        assert_eq!(0, frame.ty());
        assert_eq!(&ValueRef::Array(vec![]), frame.args());
    }

    #[test]
    #[should_panic(expected = "InvalidFrameType")]
    fn fail_invalid_frame_type() {
        let val = ValueRef::from(42);

        Frame::new(&val).unwrap();
    }

    #[test]
    #[should_panic(expected = "SpanTypeMismatch")]
    fn fail_invalid_chan_type() {
        let val = ValueRef::from(vec![
            ValueRef::from("1"),
            ValueRef::from(0),
            ValueRef::Array(vec![]),
        ]);

        Frame::new(&val).unwrap();
    }

    #[test]
    #[should_panic(expected = "TypeTypeMismatch")]
    fn fail_invalid_type_type() {
        let val = ValueRef::from(vec![
            ValueRef::from(1),
            ValueRef::from("0"),
            ValueRef::Array(vec![]),
        ]);

        Frame::new(&val).unwrap();
    }
}
