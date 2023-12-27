mod replica;
mod network;
mod client;

pub mod operations {
    pub mod quorum_info_op;
}

use std::sync::{Arc, Mutex};
use atlas_common::node_id::NodeId;

/// This is a simple observer of the quorum, which might then be extended to support
/// Other features, such as quorum reconfiguration or just keeping track of the quorum
/// (in the case of clients)
pub struct QuorumObserver {
    quorum_view: Arc<Mutex<QuorumView>>,
}

/// The current view of nodes in the network, as in which of them
/// are currently partaking in the consensus
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumView {
    sequence_number: SeqNo,

    quorum_members: Vec<NodeId>,

    f: usize,
}

impl Orderable for QuorumView {
    fn sequence_number(&self) -> SeqNo {
        self.sequence_number
    }
}

impl QuorumView {
    pub fn empty() -> Self {
        QuorumView {
            sequence_number: SeqNo::ZERO,
            quorum_members: Vec::new(),
            f: 0,
        }
    }

    pub fn with_bootstrap_nodes(bootstrap_nodes: Vec<NodeId>) -> Self {
        QuorumView {
            sequence_number: SeqNo::ZERO,
            quorum_members: bootstrap_nodes,
            f: 1,
        }
    }

    pub fn next_with_added_node(&self, node_id: NodeId, f: usize) -> Self {
        QuorumView {
            sequence_number: self.sequence_number.next(),
            quorum_members: {
                let mut members = self.quorum_members.clone();
                members.push(node_id);
                members
            },
            f,
        }
    }

    pub fn sequence_number(&self) -> SeqNo {
        self.sequence_number
    }

    pub fn quorum_members(&self) -> &Vec<NodeId> {
        &self.quorum_members
    }
}

impl QuorumObserver {
    pub fn from_bootstrap(bootstrap_nodes: Vec<NodeId>) -> Self {
        Self
        {
            quorum_view: Arc::new(Mutex::new(QuorumView::with_bootstrap_nodes(bootstrap_nodes))),
        }
    }

    pub fn current_view(&self) -> QuorumView {
        self.quorum_view.lock().unwrap().clone()
    }

    pub fn install_quorum_view(&self, view: QuorumView) {
        let guard = self.quorum_view.lock().unwrap();

        *guard = view;
    }
}

