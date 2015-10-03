//! This module is about Cocaine low level protocol handling.
//!
//! Currently we use MessagePack for serialization and deserialization.

use rmp::{Value, ValueRef};
use rmp::value::{Integer};

#[derive(Clone, Debug)]
pub enum Error {
    /// Frame must be an array.
    InvalidFrameType,
    /// Frame length must be >= 3.
    LengthMismatch(usize),
    /// Channel id type must be u64.
    ChanTypeMispatch,
    /// Type id type must be u64.
    TypeTypeMismatch,
    /// Data type must be an array.
    DataTypeMismatch,
}

/// Represents Cocaine protocol frame.
#[derive(Debug)]
pub struct Frame {
    /// Channel id.
    id: u64,
    /// Message type.
    ty: u64,
    /// Userland payload.
    data: Value,
    // TODO: meta: Vec<Value>,
}

impl Frame {
    // TODO: fn read<R>(rd: R) to completely isolate from protocol implementation (MessagePack for
    // our case).

    pub fn new(val: ValueRef) -> Result<Frame, Error> {
        if let ValueRef::Array(val) = val {
            if val.len() < 3 {
                return Err(Error::LengthMismatch(val.len()))
            }

            // TODO: Tuple match oneliner for prettiness. Actually we shouldn't check for every
            // possible error case.

            // match &val {
            //     [Integer(U64(id)), Integer(U64(ty)), Array(data)] => {
            //         let frame = Frame {
            //             id: id,
            //             ty: ty,
            //             data: data.to_owned(),
            //         };
            //
            //         Ok(frame)
            //     }
            //     other => {
            //         Err(Error::InvalidFrame)
            //     }
            // }

            let id = if let ValueRef::Integer(Integer::U64(id)) = val[0] {
                id
            } else {
                return Err(Error::ChanTypeMispatch);
            };

            let ty = if let ValueRef::Integer(Integer::U64(ty)) = val[1] {
                ty
            } else {
                return Err(Error::TypeTypeMismatch);
            };

            if let ValueRef::Array(..) = val[2] {
                // Ok.
            } else {
                return Err(Error::DataTypeMismatch);
            };

            let frame = Frame {
                id: id,
                ty: ty,
                data: val[2].to_owned(),
            };

            Ok(frame)
        } else {
            Err(Error::InvalidFrameType)
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn into_payload(self) -> (u64, Value) {
        (self.ty, self.data)
    }
}

#[cfg(test)]
mod test {
    use rmp::Value;
    use rmp::ValueRef::*;
    use rmp::value::Integer::*;

    use super::Frame;

    #[test]
    fn pass() {
        let val = Array(vec![
            Integer(U64(1)),
            Integer(U64(0)),
            Array(vec![]),
        ]);

        let frame = Frame::new(val).unwrap();

        assert_eq!(1, frame.id);
        assert_eq!(0, frame.ty);
        assert_eq!(Value::Array(vec![]), frame.data);

        assert_eq!(1, frame.id());
        assert_eq!((0, Value::Array(vec![])), frame.into_payload());
    }

    #[test]
    #[should_panic(expected = "InvalidFrameType")]
    fn fail_invalid_frame_type() {
        let val = Integer(U64(42));

        Frame::new(val).unwrap();
    }

    #[test]
    #[should_panic(expected = "ChanTypeMispatch")]
    fn fail_invalid_chan_type() {
        let val = Array(vec![
            Integer(I64(1)),
            Integer(U64(0)),
            Array(vec![]),
        ]);

        Frame::new(val).unwrap();
    }

    #[test]
    #[should_panic(expected = "TypeTypeMismatch")]
    fn fail_invalid_type_type() {
        let val = Array(vec![
            Integer(U64(1)),
            Integer(I64(0)),
            Array(vec![]),
        ]);

        Frame::new(val).unwrap();
    }

    #[test]
    #[should_panic(expected = "DataTypeMismatch")]
    fn fail_invalid_data_type() {
        let val = Array(vec![
            Integer(U64(1)),
            Integer(U64(0)),
            Integer(U64(0)),
        ]);

        Frame::new(val).unwrap();
    }
}
