use crate::quorum_config::QuorumObserver;

/// A struct detailing the information about a node that does not
/// really participate in the quorum, but requires information about it
/// in order to know who he needs to contact
pub struct ObservingQuorumNode {

    viewer: QuorumObserver,

}

