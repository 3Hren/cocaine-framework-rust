use std::convert::From;

use protocol::frame;

#[derive(Debug)]
pub enum Error {
    DecodeError, // TODO: Maybe add underlying error?
    FrameError(frame::Error),
    Io(::std::io::Error),
}

impl From<::std::io::Error> for Error {
    fn from(err: ::std::io::Error) -> Error {
        Error::Io(err)
    }
}

// NOTE: This implementation isn't much precise for I/O errors, but at least it's informative.
// When we gain an ability to clone I/O errors I remove this code.
impl Clone for Error {
    fn clone(&self) -> Error {
        match *self {
            Error::DecodeError => Error::DecodeError,
            Error::FrameError(ref err) => Error::FrameError(err.clone()),
            Error::Io(ref err) => Error::Io(::std::io::Error::new(err.kind(), format!("{}", err))),
        }
    }
}
