use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

impl<P> Message<P> {
    pub fn into_reply(self) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: None,
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    pub fn send(&self, output: &mut impl std::io::Write) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("serialize message to stdout")?;
        output.write_all(b"\n").context("write trailing newline")?;
        Ok(())
    }
}
