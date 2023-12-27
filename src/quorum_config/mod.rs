mod replica;

pub mod operations {
    pub mod quorum_info_op;
}

use atlas_common::node_id::NodeId;
use crate::quorum_reconfig::node_types::QuorumViewer;
use crate::quorum_reconfig::QuorumView;

/// This is a simple observer of the quorum, which might then be extended to support
/// Other features, such as quorum reconfiguration or just keeping track of the quorum
/// (in the case of clients)
pub struct QuorumObserver {
    viewer: QuorumViewer
}


impl QuorumObserver {

    pub fn from_bootstrap(bootstrap_nodes: Vec<NodeId>) -> Self {
        Self {
            viewer: QuorumViewer::from_bootstrap_nodes(bootstrap_nodes)
        }
    }

    pub fn current_view(&self) -> QuorumView {
        self.viewer.view()
    }

    pub fn install_quorum_view(&self, view: QuorumView) {
        self.viewer.install_view(view)
    }

}

