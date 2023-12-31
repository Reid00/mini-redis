use core::fmt;
use std::{io::Cursor, num::TryFromIntError, string::FromUtf8Error};

use bytes::{Buf, Bytes};

#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Int(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    InComplete,
    /// Invalid message encoding
    Other(crate::Error),
}

impl Frame {
    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Push a "bulk" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Push an "integer" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Int(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            // 单行字符串（Simple Strings）： 响应的首字节是 "+"
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            // 错误（Errors）： 响应的首字节是 "-"
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            // 整型（Integers）： 响应的首字节是 ":"
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            // 多行字符串（Bulk Strings）： 响应的首字节是"\$"
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Skip '-1\r\n'
                    skip(src, 4)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, len + 2)
                }
            }
            // 数组（Arrays）： 响应的首字节是 "*"
            b'*' => {
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?.to_vec();

                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?.to_vec();

                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Int(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // read the bulk string
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::InComplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?.try_into()?;

                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(resp) => resp.fmt(fmt),
            Frame::Error(err) => write!(fmt, "error: {}", err),
            Frame::Int(i) => i.fmt(fmt),
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Bulk(bytes) => match str::from_utf8(bytes) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", bytes),
            },
            Frame::Array(array) => {
                for (i, arr) in array.iter().enumerate() {
                    if i > 0 {
                        // use space as the array element display separator
                        write!(fmt, " ")?;
                    }

                    arr.fmt(fmt)?;
                }

                Ok(())
            }
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::InComplete);
    }
    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::InComplete);
    }
    Ok(src.get_u8())
}

// advance n step
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::InComplete);
    }
    src.advance(n);
    Ok(())
}

// read a new line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;
    let line = get_line(src)?;
    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

// get_line returns the line from cursor position
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    // underlying value length
    let end = src.get_ref().len() - 1;

    for i in start..=end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
        }
        return Ok(&src.get_ref()[start..i]);
    }
    Err(Error::InComplete)
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value.into())
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_value: FromUtf8Error) -> Self {
        "protocol error; invalid frame formt".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_value: TryFromIntError) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InComplete => "stream ended early".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}
