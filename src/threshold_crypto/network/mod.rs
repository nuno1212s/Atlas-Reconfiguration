use std::sync::Arc;
use atlas_common::node_id::NodeId;
use atlas_common::error::*;
use atlas_communication::reconfiguration_node::ReconfigurationNode;
use crate::message::{ReconfData, ReconfigurationMessage, ThresholdMessages};

pub trait ThresholdNetwork {
    /// Send a threshold protocol message
    fn send_threshold_message(&self, threshold_message: ThresholdMessages, target: NodeId) -> Result<()>;

    /// Broadcast a threshold protocol message
    fn broadcast_threshold_message(&self, threshold_messages: ThresholdMessages, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;
}

pub struct ThresholdNetworkWrapper<NT> {
    node: Arc<NT>,
}

impl<NT> ThresholdNetwork for ThresholdNetworkWrapper<NT> where NT: ReconfigurationNode<ReconfData> + 'static {
    fn send_threshold_message(&self, threshold_message: ThresholdMessages, target: NodeId) -> Result<()> {
        let reconf_message = ReconfigurationMessage::ThresholdCrypto(threshold_message);

        self.node.send_reconfig_message(reconf_message, target)
    }

    fn broadcast_threshold_message(&self, threshold_messages: ThresholdMessages, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let reconf_message = ReconfigurationMessage::ThresholdCrypto(threshold_messages);

        self.node.broadcast_reconfig_message(reconf_message, target)
    }
}

impl<NT> From<Arc<NT>> for ThresholdNetworkWrapper<NT>
    where NT: ReconfigurationNode<ReconfData> + 'static {
    fn from(value: Arc<NT>) -> Self {
        Self {
            node: value
        }
    }
}