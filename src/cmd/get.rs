/// Get the value of key
///
/// if key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handle string values as key.

pub struct Get {
    /// Name of the key to get
    key: String,
}

impl Get {
    /// Create a new "Get" Command which fetches 'key'.
    pub fn new(key: impl ToString) -> Self {
        Get {
            key: key.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        
    }
}
