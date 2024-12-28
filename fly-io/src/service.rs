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

pub const LINEAR_STORE_ADDRESS: &str = "lin-kv";
pub const SEQUENTIAL_STORE_ADDRESS: &str = "seq-kv";
pub const STORAGE_ADDRESSES: [&str; 2] = [LINEAR_STORE_ADDRESS, SEQUENTIAL_STORE_ADDRESS];

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
    _node_id: String,
}

impl SequentialStore {
    pub fn new(node_id: String) -> Self {
        Self { _node_id: node_id }
    }
}

impl Storage<()> for SequentialStore {
    fn node_id(&self) -> String {
        self._node_id.clone()
    }

    fn address(&self) -> String {
        SEQUENTIAL_STORE_ADDRESS.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct LinearStore {
    _node_id: String,
}

impl LinearStore {
    pub fn new(node_id: String) -> Self {
        Self { _node_id: node_id }
    }
}

impl Storage<()> for LinearStore {
    fn node_id(&self) -> String {
        self._node_id.clone()
    }

    fn address(&self) -> String {
        LINEAR_STORE_ADDRESS.to_string()
    }
}

#[async_trait::async_trait]
pub trait Storage<IP>: Send
where
    IP: Send + Debug + Clone + 'static,
{
    fn node_id(&self) -> String;
    fn address(&self) -> String; // Should be static but needs a receiver to implement Send.

    async fn read<T>(&self, key: String, network: &Network<IP>) -> anyhow::Result<T>
    where
        IP: Send + Debug + Clone + 'static,
        T: DeserializeOwned,
    {
        let message = self.construct_message(self.node_id().clone(), StoragePayload::Read { key });
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

    fn write<T>(&self, key: String, value: T, network: &Network<IP>) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let message = self.construct_message(
            self.node_id().clone(),
            StoragePayload::Write {
                key,
                value: serde_json::to_value(value).expect("failed to serialize value"),
            },
        );

        network.send(message).context("writing value for key")?;
        Ok(())
    }

    async fn compare_and_store<T>(
        &self,
        key: String,
        from: T,
        to: T,
        network: &Network<IP>,
    ) -> anyhow::Result<()>
    where
        T: Serialize + Send,
    {
        let message = self.construct_message(
            self.node_id().clone(),
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

    fn construct_message<PAYLOAD>(&self, node_id: String, payload: PAYLOAD) -> Message<PAYLOAD> {
        Message {
            src: node_id,
            dst: self.address(),
            body: Body {
                id: None,
                in_reply_to: None,
                payload,
            },
        }
    }
}
