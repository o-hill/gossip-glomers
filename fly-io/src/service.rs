use std::fmt::Debug;

use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{network::Network, Body, Message};

pub type Entry = usize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StorageType {
    Uint(Entry),
    Array(Vec<Entry>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StoragePayload {
    Read {
        key: String,
    },
    ReadOk {
        value: serde_json::Value,
    },
    Write {
        key: String,
        value: serde_json::Value,
    },
    WriteOk,
    Cas {
        key: String,
        from: serde_json::Value,
        to: serde_json::Value,
        create_if_not_exists: Option<bool>,
    },
    CasOk,
    Error {
        code: usize,
        text: String,
    },
}

#[derive(Debug, Clone)]
pub struct SequentialStore {
    node_id: String,
}

impl SequentialStore {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    pub fn address() -> String {
        "seq-kv".to_string()
    }

    pub async fn read<T, IP>(&self, key: String, network: &mut Network<IP>) -> anyhow::Result<T>
    where
        IP: Send + Debug + Clone + 'static,
        T: DeserializeOwned,
    {
        let message = Self::construct_message(self.node_id.clone(), StoragePayload::Read { key });
        let response = network
            .request(message)
            .await
            .context("fetching value for key")?;

        match response.body.payload {
            StoragePayload::ReadOk { value } => {
                serde_json::from_value(value).context("deserializing read value")
            }
            _ => Err(anyhow::anyhow!("error returned from read request")),
        }
    }

    pub fn write<T, IP>(
        &self,
        key: String,
        value: T,
        network: &mut Network<IP>,
    ) -> anyhow::Result<()>
    where
        IP: Send + Debug + Clone + 'static,
        T: Serialize,
    {
        let message = Self::construct_message(
            self.node_id.clone(),
            StoragePayload::Write {
                key,
                value: serde_json::to_value(value).expect("failed to serialize value"),
            },
        );

        network.send(message).context("writing value for key")?;
        Ok(())
    }

    pub async fn compare_and_store<T, IP>(
        &self,
        key: String,
        from: T,
        to: T,
        network: &mut Network<IP>,
    ) -> anyhow::Result<()>
    where
        IP: Send + Debug + Clone + 'static,
        T: Serialize,
    {
        let message = Self::construct_message(
            self.node_id.clone(),
            StoragePayload::Cas {
                key,
                from: serde_json::to_value(from).expect("failed to serialize from"),
                to: serde_json::to_value(to).expect("failed to serialize to"),
                create_if_not_exists: Some(true),
            },
        );

        let response = network
            .request(message)
            .await
            .context("writing value for key")?;

        match response.body.payload {
            StoragePayload::CasOk => Ok(()),
            _ => Err(anyhow::anyhow!("error returned from cas request")),
        }
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
}

pub struct LinearStore {}

impl LinearStore {
    pub fn address() -> String {
        "lin-kv".to_string()
    }
}
