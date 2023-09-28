use crate::Connection;
use crate::Frame;

use tracing::{debug, instrument};

/// Represents an "unknown" command. This is not a real `Redis` command.
#[derive(Debug)]
pub struct Unknown {
    cmd_name: String,
}

impl Unknown {
    /// Create a new `Unknown` command which responds to unknown commands
    /// issued by clients
    pub(crate) fn new(key: impl ToString) -> Self {
        Self {
            cmd_name: key.to_string(),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.cmd_name
    }

    /// Responds to the client, indicating the command is not recognized.
    ///
    /// This usually means the command is not yet implemented by `mini-redis`.
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let resp = Frame::Error(format!("err unknown command '{}'", self.cmd_name));

        debug!(?resp);

        dst.write_frame(&resp).await?;
        Ok(())
    }
}
