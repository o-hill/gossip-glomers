use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    network::Network,
    protocol::{Body, Message},
    Event,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StoragePayload {
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    Write {
        key: String,
        value: usize,
    },
    WriteOk,
    Cas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: Option<bool>,
    },
    CasOk,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StorageType {
    Array(Vec<usize>),
    Uint(usize),
}

#[derive(Debug, Clone)]
pub struct SequentialStore {}

impl SequentialStore {
    pub fn address() -> String {
        "seq-kv".to_string()
    }

    pub fn key() -> String {
        "value".to_string()
    }

    fn read() -> StoragePayload {
        StoragePayload::Read { key: Self::key() }
    }

    pub fn compare_and_store(from: usize, to: usize) -> StoragePayload {
        StoragePayload::Cas {
            key: Self::key(),
            from,
            to,
            create_if_not_exists: Some(true),
        }
    }

    fn write(value: usize) -> StoragePayload {
        StoragePayload::Write {
            key: Self::key(),
            value,
        }
    }

    pub fn initialize<P, IP>(node_id: String, network: &mut Network<P, IP>) -> anyhow::Result<()>
    where
        P: Serialize + Clone + Send + DeserializeOwned + 'static,
        IP: Clone + Send + 'static,
    {
        network
            .send(Self::construct_message(node_id, Self::write(0)))
            .context("initializing storage value")
    }

    pub fn construct_message<PAYLOAD>(node_id: String, payload: PAYLOAD) -> Message<PAYLOAD> {
        Message {
            src: node_id,
            dst: Self::address(),
            body: Body {
                id: None,
                in_reply_to: None,
                payload,
            },
        }
    }

    pub async fn read_current_value<P, IP>(
        node_id: String,
        network: &mut Network<P, IP>,
    ) -> anyhow::Result<Message<P>>
    where
        P: Serialize + Clone + Send + DeserializeOwned + 'static,
        IP: Clone + Send + 'static,
    {
        let event = network
            .request(Self::construct_message(node_id, SequentialStore::read()))
            .await
            .context("fetching current value")?;

        let Event::Message(message) = event else {
            return Err(anyhow::anyhow!(
                "value request event response was not a message"
            ));
        };

        Ok(message)
    }
}

pub struct LinearStore {}

impl LinearStore {
    pub fn address() -> String {
        "lin-kv".to_string()
    }
}
