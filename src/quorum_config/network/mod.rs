use std::sync::Arc;
use atlas_common::node_id::NodeId;
use atlas_common::error::*;
use atlas_common::ordering::SeqNo;
use atlas_communication::reconfiguration_node::ReconfigurationNode;
use crate::message::{QuorumJoinReconfMessages, ReconfData, ReconfigurationMessage, ReconfigurationMessageType};

pub trait QuorumConfigNetworkNode {
    /// Send a participating quorum message to a specific node
    fn send_quorum_config_message(&self, seq: SeqNo, message: QuorumJoinReconfMessages, target: NodeId) -> Result<()>;

    /// Broadcast a participating quorum message to a set of nodes
    fn broadcast_quorum_message(&self, seq: SeqNo, message: QuorumJoinReconfMessages, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;
}

pub struct QuorumConfigNetworkWrapper<NT> {
    node: Arc<NT>,
}

impl<NT> QuorumConfigNetworkNode for QuorumConfigNetworkWrapper<NT>
    where NT: ReconfigurationNode<ReconfData> + 'static {
    fn send_quorum_config_message(&self, seq: SeqNo, message: QuorumJoinReconfMessages, target: NodeId) -> Result<()> {

        let reconf_message = ReconfigurationMessage::new(seq, ReconfigurationMessageType::QuorumConfig(message));

        self.node.send_reconfig_message(reconf_message, target)
    }

    fn broadcast_quorum_message(&self, seq: SeqNo, message: QuorumJoinReconfMessages, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {

        let reconf_message = ReconfigurationMessage::new(seq, ReconfigurationMessageType::QuorumConfig(message));

        self.node.broadcast_reconfig_message(reconf_message, target)
    }
}

impl<NT> From<Arc<NT>> for QuorumConfigNetworkWrapper<NT>
    where NT: ReconfigurationNode<ReconfData> + 'static {
    fn from(node: Arc<NT>) -> Self {
        Self {
            node
        }
    }
}