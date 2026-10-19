//! The gRPC entry point: tonic calls these methods for every incoming request.
//! Each handler stays thin (unwrap the message, count it, later check `muted`)
//! and hands the real work to a `handle_*` function: this is the dispatch point
//! where other modules plug in.

use std::sync::Arc;

use common::pb::node_server::Node;
use common::pb::{
    FaultAck, FaultCommand, HeartbeatReply, HeartbeatRequest, SubmitReply, SubmitRequest,
};
use tonic::{Request, Response, Status};

use crate::state::NodeState;

pub struct NodeService {
    /// Shared with the background tasks (heartbeat loop, detector ticker).
    state: Arc<NodeState>,
}

impl NodeService {
    pub fn new(state: Arc<NodeState>) -> Self {
        NodeService { state }
    }

    /// Task 4's gate. Muted: hold the request until the mute ends, then drop it.
    /// Held, not refused: a crashed machine never answers "I'm down"; callers only
    /// notice when their own deadline runs out (and then cancel, which ends the wait).
    async fn gate(&self) -> Result<(), Status> {
        if !self.state.is_muted() {
            return Ok(());
        }
        self.state.wait_while_muted().await;
        Err(Status::unavailable("dropped: node was muted"))
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        self.gate().await?; // muted: arriving requests are held
        let hb = req.into_inner();
        // debug, not info: 2 peers x 2 per second would flood the log. Show with RUST_LOG=debug.
        tracing::debug!(from = hb.from_node, seq = hb.seq, "heartbeat received");
        // Task 5: tell the failure detector we heard from hb.from_node.
        let reply = HeartbeatReply {
            node_id: self.state.id,
            seq: hb.seq,
        };
        self.gate().await?; // muted meanwhile: the reply never leaves
        Ok(Response::new(reply))
    }

    async fn submit(&self, req: Request<SubmitRequest>) -> Result<Response<SubmitReply>, Status> {
        self.gate().await?; // before counting: held requests are not work
        let _guard = self.state.start_request(); // P3's queue counter, until this returns
        let reply = handle_submit(&self.state, req.into_inner()).await?;
        // Muted mid-request: drop the reply. The client retries with the same
        // request_id and gets the stored plan (duplicate = true).
        self.gate().await?;
        Ok(reply)
    }

    async fn inject_fault(&self, req: Request<FaultCommand>) -> Result<Response<FaultAck>, Status> {
        self.gate().await?; // a muted node ignores new fault orders too
        // No gate before replying: this handler mutes the node itself and must still ack.
        handle_inject_fault(&self.state, req.into_inner()).await
    }
}

/// Task 7 (Tue): check request_id -> incident_from_pb -> store.snapshot() -> plan()
/// -> store.apply() -> reply.
async fn handle_submit(
    _state: &NodeState,
    _req: SubmitRequest,
) -> Result<Response<SubmitReply>, Status> {
    Err(Status::unimplemented("Submit is wired in task 7"))
}

/// Task 4: send the FaultAck, then go silent for duration_ms.
async fn handle_inject_fault(
    _state: &NodeState,
    _cmd: FaultCommand,
) -> Result<Response<FaultAck>, Status> {
    Err(Status::unimplemented("InjectFault is wired in task 4"))
}

#[cfg(test)]
mod tests {
    use common::config::ClusterConfig;

    use super::*;
    use crate::peers::Peers;

    fn service() -> NodeService {
        let cfg = ClusterConfig::parse("[[nodes]]\nid = 1\naddr = \"127.0.0.1:50051\"\n").unwrap();
        let peers = Peers::new(&cfg, 1).unwrap();
        NodeService::new(Arc::new(NodeState::new(1, cfg, peers)))
    }

    #[tokio::test]
    async fn heartbeat_replies_with_own_id_and_echoes_seq() {
        let svc = service();
        let reply = svc
            .heartbeat(Request::new(HeartbeatRequest { from_node: 2, seq: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_id, 1);
        assert_eq!(reply.seq, 7);
    }

    #[tokio::test]
    async fn submit_is_a_stub_and_leaves_counter_at_zero() {
        let svc = service();
        let err = svc
            .submit(Request::new(SubmitRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert_eq!(svc.state.in_flight(), 0);
    }
}
