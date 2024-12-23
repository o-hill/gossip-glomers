use std::io::BufRead;

use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

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
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|i| {
                    let ret = *i;
                    *i += 1;
                    ret
                }),
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

pub trait Node<PAYLOAD> {
    fn from_init(init: Init) -> Self;
    fn step(
        &mut self,
        input: Message<PAYLOAD>,
        output: &mut impl std::io::Write,
    ) -> anyhow::Result<()>;
}

pub fn run<PAYLOAD, NODE>() -> anyhow::Result<()>
where
    PAYLOAD: DeserializeOwned,
    NODE: Node<PAYLOAD>,
{
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("could not read from stdin")
            .context("failed to read init message from stdin")?,
    )
    .context("failed to deserialize init message")?;

    let InitPayload::Init(init) = init_msg.body.payload.clone() else {
        panic!("first message was not an init");
    };

    let mut node = NODE::from_init(init);

    let mut reply = init_msg.into_reply(Some(&mut 0));
    reply.body.payload = InitPayload::InitOk;
    reply.send(&mut stdout).context("sending init_ok message")?;

    for input in stdin {
        let input = input.context("Maelstrom event could not be read from stdin")?;
        let message: Message<PAYLOAD> = serde_json::from_str(input.as_str())
            .context("failed to deserialize maelstrom input")?;

        node.step(message, &mut stdout)
            .context("node failed to process message")?;
    }

    Ok(())
}
