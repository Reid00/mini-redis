use std::pin::Pin;

use bytes::Bytes;

use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::{
    cmd::{Parse, ParseError, Unknown},
    db::Db,
    Connection, Frame,
};

/// Subscribes the client to one or more channels.
///
/// Once the client enters the subscribed state, it is not supposed to issue any
/// other commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
/// PUNSUBSCRIBE, PING and QUIT commands.
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// Unsubscribes the client from one or more channels.
///
/// When no channels are specified, the client is unsubscribed from all the
/// previously subscribed channels.
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// Stream of messages. The stream receives messages from the
/// `broadcast::Receiver`. We use `stream!` to create a `Stream` that consumes
/// messages. Because `stream!` values cannot be named, we box the stream using
/// a trait object.
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    pub(crate) fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    /// Parse a `Subscribe` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SUBSCRIBE` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Subscribe` value is returned. If the frame is
    /// malformed, `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two or more entries.
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        // The `SUBSCRIBE` string has already been consumed. At this point,
        // there is one or more strings remaining in `parse`. These represent
        // the channels to subscribe to.
        //
        // Extract the first string. If there is none, the the frame is
        // malformed and the error is bubbled up.
        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }
}

async fn subscribe_to_channel(
    chan_name: String,
    subscription: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(chan_name.clone());

    // Subscribe to the channel.
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // If we lagged in consuming messages, just resume.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    subscription.insert(chan_name.clone(), rx);

    let resp = make_subscibe_frame(chan_name, subscription.len());
    dst.write_frame(&resp).await?;
    Ok(())
}

/// Creates the response to a subcribe request.
///
/// All of these functions take the `channel_name` as a `String` instead of
/// a `&str` since `Bytes::from` can reuse the allocation in the `String`, and
/// taking a `&str` would require copying the data. This allows the caller to
/// decide whether to clone the channel name or not.
fn make_subscibe_frame(chan_name: String, num_subs: usize) -> Frame {
    let mut resp = Frame::array();
    resp.push_bulk(Bytes::from_static(b"subscribe"));
    resp.push_bulk(Bytes::from(chan_name));
    resp.push_int(num_subs as u64);
    resp
}

/// Creates the response to an unsubcribe request.
fn make_unsubscribe_frame(chan_name: String, num_subs: usize) -> Frame {
    let mut resp = Frame::array();
    resp.push_bulk(Bytes::from_static(b"unsubscribe"));
    resp.push_bulk(Bytes::from(chan_name));
    resp.push_int(num_subs as u64);
    resp
}

/// Creates a message informing the client about a new message on a channel that
/// the client subscribes to.
fn make_message_frame(chan_name: String, msg: Bytes) -> Frame {
    let mut resp = Frame::array();

    resp.push_bulk(Bytes::from_static(b"message"));
    resp.push_bulk(Bytes::from(chan_name));
    resp.push_bulk(msg);
    resp
}
