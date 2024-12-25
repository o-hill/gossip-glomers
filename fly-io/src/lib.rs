use protocol::Message;
use serde::de::DeserializeOwned;

pub mod protocol;
pub mod server;

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
}

#[async_trait::async_trait]
pub trait Node<Payload, InjectedPayload = ()>
where
    Payload: Clone,
    InjectedPayload: Clone,
{
    fn from_init(
        init: crate::protocol::Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> Self;
    async fn step(
        &mut self,
        event: Event<Payload, InjectedPayload>,
        network: &mut crate::server::Network<Payload, InjectedPayload>,
    ) -> anyhow::Result<()>;
}
