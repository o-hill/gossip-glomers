use protocol::{UntypedBody, UntypedMessage};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use service::{StoragePayload, STORAGE_ADDRESSES};

pub mod network;
pub mod protocol;
pub mod server;
pub mod service;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Body<P> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: P,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

impl<PAYLOAD> Message<PAYLOAD>
where
    PAYLOAD: Serialize,
{
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
}

impl<PAYLOAD> From<UntypedMessage> for Message<PAYLOAD>
where
    PAYLOAD: DeserializeOwned,
{
    fn from(untyped: UntypedMessage) -> Self {
        let payload = serde_json::from_value(untyped.body.payload)
            .expect("could not deserialize payload into provided type");
        Self {
            src: untyped.src,
            dst: untyped.dst,
            body: Body {
                id: untyped.body.id,
                in_reply_to: untyped.body.in_reply_to,
                payload,
            },
        }
    }
}

impl<PAYLOAD> From<Message<PAYLOAD>> for UntypedMessage
where
    PAYLOAD: Serialize,
{
    fn from(value: Message<PAYLOAD>) -> Self {
        let payload = serde_json::to_value(value.body.payload).expect("serializing payload");
        Self {
            src: value.src,
            dst: value.dst,
            body: UntypedBody {
                id: value.body.id,
                in_reply_to: value.body.in_reply_to,
                payload,
            },
        }
    }
}

pub enum NetworkEvent<InjectedPayload = ()> {
    Message(UntypedMessage),
    Injected(InjectedPayload),
}

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    Storage(Message<StoragePayload>),
}

impl<P, IP> From<NetworkEvent<IP>> for Event<P, IP>
where
    P: DeserializeOwned,
{
    fn from(value: NetworkEvent<IP>) -> Self {
        match value {
            NetworkEvent::Message(untyped) => {
                if STORAGE_ADDRESSES.contains(&untyped.dst.as_str())
                    || STORAGE_ADDRESSES.contains(&untyped.src.as_str())
                {
                    let typed: Message<StoragePayload> = Message::from(untyped);
                    return Event::Storage(typed);
                }
                let typed: Message<P> = Message::from(untyped);
                Event::Message(typed)
            }
            NetworkEvent::Injected(payload) => Event::Injected(payload),
        }
    }
}

#[async_trait::async_trait]
pub trait Node<Payload, InjectedPayload = ()>
where
    InjectedPayload: Clone,
{
    fn from_init(
        init: crate::protocol::Init,
        network: &crate::network::Network<InjectedPayload>,
    ) -> Self;
    async fn step(
        &mut self,
        event: Event<Payload, InjectedPayload>,
        network: &crate::network::Network<InjectedPayload>,
    ) -> anyhow::Result<()>;
}
