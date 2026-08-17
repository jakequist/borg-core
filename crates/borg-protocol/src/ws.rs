//! **The WebSocket transport: one protocol, the other framing.** SPEC.md §17.4, §17.6.
//!
//! A unix socket is a byte stream, so §17.4's framing is what turns it into messages — a newline for
//! JSON, a length prefix for MessagePack. A WebSocket is *already* a stream of messages, so the
//! framing has nothing left to do, and wrapping it anyway would put two delimiters around one
//! message and leave the inner one being written by a sender nobody parses it from.
//!
//! So the framing layer **disappears** here rather than being wrapped, and what is shared is the
//! encoding: [`crate::encode_message`] and [`crate::decode_message`] produce the identical bytes for
//! the identical types, and this module only decides which kind of frame they travel in.
//!
//! **A codec picks the frame kind.** JSON is text and MessagePack is binary, which is what the two
//! frame kinds are *for*: a browser's `event.data` is a `string` for the first and a `Blob` for the
//! second with no configuration, and a proxy or a debugger that prints a text frame prints the same
//! NDJSON line a shell client would have written. Sending JSON in a binary frame would work and
//! would make every tool between here and the client show bytes.
//!
//! ## What is *not* here
//!
//! TLS. `borg-server` speaks plaintext `ws://` and expects a proxy in front of it to terminate
//! (§17.6), so `tungstenite` is taken with `default-features = false` and no TLS backend at all —
//! which is the scope decision made mechanical rather than merely documented, since there is no
//! certificate store in the binary to be reached by a later `wss://` by accident.
//!
//! ## The dependency
//!
//! `tungstenite`, blocking, no TLS, `handshake` only. The serve loop is thread-per-connection and
//! synchronous (`borg_server::serve`), so the blocking form is the one that matches; the async form
//! would mean a second framing implementation on the server, which is the thing `borg-protocol`
//! exists to prevent.
//!
//! It is a dependency rather than a hand-rolled frame reader because RFC 6455 is somebody else's
//! wire and the *client* is a browser: masking, continuation frames, control frames interleaved
//! mid-message, close handshakes and payload-length limits are all things the peer will do whether
//! or not this implementation expects them, and each is a place where a hand-rolled reader is wrong
//! in a way no test here would have written. It shares the `digest`/`sha1` stack `sha2` already
//! brings, and `handshake` is the only feature enabled.

use crate::{Codec, ProtocolError};
use serde::{Deserialize, Serialize};
use std::net::TcpStream;
use tungstenite::{Message, WebSocket};

/// One message as the frame its codec travels in. See the module header.
pub fn frame<T: Serialize>(codec: Codec, message: &T) -> Result<Message, ProtocolError> {
    let body = crate::encode_message(codec, message)?;
    Ok(match codec {
        Codec::Json => Message::text(
            String::from_utf8(body)
                .map_err(|err| ProtocolError::Encoding(format!("json is not utf-8: {err}")))?,
        ),
        Codec::Msgpack => Message::binary(body),
    })
}

/// What a received frame carries, or `None` for a control frame the caller should skip.
///
/// Ping and Pong are `None` rather than an error because `tungstenite` answers a Ping itself and a
/// Pong is a browser keeping a proxy's idle timer alive — neither is a protocol event, and a peer
/// that treated one as a message would drop a request every time an intermediary got bored.
pub fn payload(message: &Message) -> Option<Vec<u8>> {
    match message {
        Message::Text(text) => Some(text.as_bytes().to_vec()),
        Message::Binary(bytes) => Some(bytes.to_vec()),
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => None,
    }
}

/// Whether a frame is the peer saying goodbye.
#[must_use]
pub fn is_close(message: &Message) -> bool {
    matches!(message, Message::Close(_))
}

/// A client's end of a WebSocket to a `borg-server`. The dial half of §17.5's `ask`.
pub struct Client(WebSocket<TcpStream>);

impl Client {
    /// Dial `host:port` and complete the WebSocket upgrade.
    ///
    /// Plaintext only: TLS is terminated by a proxy in front of the server (§17.6), and
    /// `crate::url::Address` is what refuses `borg+wss://` here with a sentence saying so.
    pub fn dial(host: &str, port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;
        // The request url comes from `Address`, which is where the decision that its path is `/`
        // and carries no registry lives (§17.6). Formatting a second one here would be a second
        // place for that to be decided.
        let asking = crate::url::Address::Ws {
            secure: false,
            host: host.to_string(),
            port,
        };
        let url = asking.ws_url().unwrap_or_default();
        let (socket, _) = tungstenite::client(url, stream)
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        Ok(Self(socket))
    }

    pub fn send<T: Serialize>(&mut self, codec: Codec, message: &T) -> Result<(), ProtocolError> {
        self.0
            .send(frame(codec, message)?)
            .map_err(|err| closed_or(&err))
    }

    pub fn recv<T: for<'de> Deserialize<'de>>(&mut self, codec: Codec) -> Result<T, ProtocolError> {
        loop {
            let message = self.0.read().map_err(|err| closed_or(&err))?;
            if is_close(&message) {
                return Err(ProtocolError::Closed);
            }
            if let Some(body) = payload(&message) {
                return crate::decode_message(codec, &body);
            }
        }
    }

    /// Say goodbye properly. A close frame the server can see is what stops an abandoned session
    /// looking, from the other end, like a client that crashed.
    ///
    /// **Bounded**, because the drain below is a read on a peer that may never answer: a deadline is
    /// what keeps "being polite on the way out" from being a thread that never returns.
    pub fn close(mut self) {
        let _ = self.0.get_ref().set_read_timeout(Some(GOODBYE));
        let _ = self.0.close(None);
        // Drain until the peer's close comes back or the socket ends; anything else here is not
        // news, because we are leaving.
        for _ in 0..16 {
            if self.0.read().is_err() {
                break;
            }
        }
    }
}

/// How long a close waits for the peer's answering Close frame. See [`Client::close`] and
/// `borg_server::serve`'s refusal path: an unbounded drain lets a silent peer pin a thread.
pub const GOODBYE: std::time::Duration = std::time::Duration::from_millis(500);

/// A `tungstenite` error as a protocol one. A peer that has gone is [`ProtocolError::Closed`],
/// which is what every caller already branches on, and everything else keeps its own words.
pub fn closed_or(err: &tungstenite::Error) -> ProtocolError {
    match err {
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            ProtocolError::Closed
        }
        tungstenite::Error::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
            ProtocolError::Closed
        }
        other => ProtocolError::Encoding(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **A codec picks the frame kind, and the bytes are the ones the byte-stream transport would
    /// have carried.** This is the whole claim of the module: one encoding, two framings, and the
    /// framing is the only thing a transport chooses.
    #[test]
    fn json_is_a_text_frame_and_msgpack_is_a_binary_one() {
        let message = crate::client::Request::BranchList {};

        let text = frame(Codec::Json, &message).unwrap();
        assert!(matches!(text, Message::Text(_)));
        assert_eq!(
            String::from_utf8(payload(&text).unwrap()).unwrap(),
            r#"{"branch_list":{}}"#,
            "a text frame carries the line a shell client would have written, without the newline"
        );

        let binary = frame(Codec::Msgpack, &message).unwrap();
        assert!(matches!(binary, Message::Binary(_)));
        assert_eq!(
            payload(&binary).unwrap(),
            crate::encode_message(Codec::Msgpack, &message).unwrap(),
            "…and a binary frame carries exactly what the length prefix would have wrapped"
        );
    }

    /// A control frame is not a message. A peer that read a Pong as one would drop a request every
    /// time a proxy sent a keepalive.
    #[test]
    fn a_control_frame_carries_no_message() {
        assert!(payload(&Message::Ping(Vec::new().into())).is_none());
        assert!(payload(&Message::Pong(Vec::new().into())).is_none());
        assert!(payload(&Message::Close(None)).is_none());
    }
}
