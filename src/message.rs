use rmpv::ValueRef;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Error {
    TypeMismatch,
    SizeMismatch(usize),
}

pub struct Message<'a> {
    id: u64,
    ty: u64,
    args: Vec<ValueRef<'a>>,
    meta: Vec<ValueRef<'a>>,
}

impl<'a> Message<'a> {
    pub fn new(frame: ValueRef<'a>) -> Result<Self, Error> {
        if let ValueRef::Array(mut vec) = frame {
            match vec.len() {
                3 => {
                    let id = vec[0].as_u64().ok_or(Error::TypeMismatch)?;
                    let ty = vec[1].as_u64().ok_or(Error::TypeMismatch)?;
                    let args = vec.pop().unwrap().into_array().ok_or(Error::TypeMismatch)?;

                    let message = Message {
                        id: id,
                        ty: ty,
                        args: args,
                        meta: Vec::new(),
                    };

                    Ok(message)
                }
                4 => {
                    unimplemented!();
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

    pub fn args(&self) -> &Vec<ValueRef> {
        &self.args
    }
}
