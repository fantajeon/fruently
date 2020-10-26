//! Send record(s) into Fluentd.

use crate::error::FluentError;
#[cfg(not(feature = "time-as-integer"))]
use crate::event_record::EventRecord;
use crate::forwardable::forward::Forward;
use crate::record::Record;
use crate::retry_conf::RetryConf;
use rmp_serde::encode::Serializer;
use serde::ser::Serialize;
use serde_json;
use std::time::Duration;
use std::borrow::{Borrow, Cow};
use std::io::Write;
use std::net;
use std::net::ToSocketAddrs;

#[derive(Debug, Clone, PartialEq)]
pub struct Fluent<'a, A>
where
    A: ToSocketAddrs,
{
    addr: A,
    tag: Cow<'a, str>,
    conf: RetryConf,
}

#[cfg(feature = "time-as-integer")]
type MsgPackSendType<T> = Record<T>;
#[cfg(not(feature = "time-as-integer"))]
type MsgPackSendType<T> = EventRecord<T>;

impl<'a, A: ToSocketAddrs> Fluent<'a, A> {
    /// Create Fluent type.
    ///
    /// ### Usage
    ///
    /// ```
    /// use fruently::fluent::Fluent;
    /// let fruently_with_str_tag = Fluent::new("127.0.0.1:24224", "test");
    /// let fruently_with_string_tag = Fluent::new("127.0.0.1:24224", "test".to_string());
    /// ```
    pub fn new<T>(addr: A, tag: T) -> Fluent<'a, A>
    where
        T: Into<Cow<'a, str>>,
    {
        Fluent {
            addr,
            tag: tag.into(),
            conf: RetryConf::new(),
        }
    }

    pub fn new_with_conf<T>(addr: A, tag: T, conf: RetryConf) -> Fluent<'a, A>
    where
        T: Into<Cow<'a, str>>,
    {
        Fluent {
            addr,
            tag: tag.into(),
            conf,
        }
    }

    #[doc(hidden)]
    pub fn get_addr(&self) -> &A {
        self.addr.borrow()
    }

    #[doc(hidden)]
    pub fn get_tag(&'a self) -> Cow<'a, str> {
        Cow::Borrowed(&self.tag)
    }

    #[doc(hidden)]
    pub fn get_conf(&self) -> Cow<RetryConf> {
        Cow::Borrowed(&self.conf)
    }

    #[doc(hidden)]
    /// For internal usage.
    pub fn closure_send_as_json<T: Serialize>(
        addr: &A, record: &Record<T>,
    ) -> Result<(), FluentError>
    {
        let sockaddr = addr.to_socket_addrs();
        let sock  = sockaddr?.next().unwrap();
        let result = net::TcpStream::connect_timeout(&sock, Duration::from_secs(1));
        match result {
            Ok(mut stream) => {
                let message = serde_json::to_string(&record)?;
                stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                let _result = stream.write(&message.into_bytes());
                drop(stream);
                return Ok(());
            },
            Err(v) => {
                println!("Failed to Connect: {:?}", v);
                return Err(From::from(v));
            },
        }
    }

    #[doc(hidden)]
    /// For internal usage.
    pub fn closure_send_as_msgpack<T: Serialize>(
        addr: &A, record: &MsgPackSendType<T>,
    ) -> Result<(), FluentError> {
        let mut stream = net::TcpStream::connect(addr)?;
        let result = record.serialize(&mut Serializer::new(&mut stream));

        match result {
            Ok(_) => Ok(()),
            Err(v) => Err(From::from(v)),
        }
    }

    #[doc(hidden)]
    /// For internal usage.
    pub fn closure_send_as_forward<T: Serialize>(
        addr: &A, forward: &Forward<T>,
    ) -> Result<(), FluentError> {
        let mut stream = net::TcpStream::connect(addr)?;
        let result = forward.serialize(&mut Serializer::new(&mut stream));

        match result {
            Ok(_) => Ok(()),
            Err(v) => Err(From::from(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry_conf::RetryConf;
    use std::borrow::Cow;

    #[test]
    fn create_fruently() {
        let fruently = Fluent::new("127.0.0.1:24224", "test");
        let expected = Fluent {
            addr: "127.0.0.1:24224",
            tag: Cow::Borrowed("test"),
            conf: RetryConf::new(),
        };
        assert_eq!(expected, fruently);
    }
}
