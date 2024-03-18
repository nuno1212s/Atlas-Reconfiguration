use atlas_common::node_id::NodeType;
use atlas_common::{crypto::signature::KeyPair, node_id::NodeId, peer_addr::PeerAddr};
use atlas_communication::reconfiguration::NodeInfo;

/// The configuration for the reconfiguration network.
pub struct ReconfigurableNetworkConfig {
    // The node ID of this node
    pub node_id: NodeId,
    // The type of this node.
    pub node_type: NodeType,
    // The key pair of this node
    pub key_pair: KeyPair,
    // Our address
    pub our_address: PeerAddr,
    // The nodes that we already know about (Boostrap nodes of the network)
    pub known_nodes: Vec<NodeInfo>,
}
