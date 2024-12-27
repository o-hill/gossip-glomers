use serde::{Deserialize, Serialize};

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

pub struct LinearStore {}

impl LinearStore {
    pub fn address() -> String {
        "lin-kv".to_string()
    }
}
