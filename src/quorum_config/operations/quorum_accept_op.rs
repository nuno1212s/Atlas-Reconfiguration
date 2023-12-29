use crate::message::LockedQC;
use crate::quorum_config::QuorumView;

/// The operation to accept a new node into the quorum
pub struct QuorumAcceptNodeOperation {

    received_locked_qc: Option<LockedQC>,

    // The current phase of our protocol
    phase: OperationPhase
}

/// The phase of a replica in the quorum reconfiguration protocol
pub enum OperationPhase {
    // We are waiting for a new member to try to join our quorum
    Waiting,
    // The new member has requested to join our quorum and we have now locked onto the new quorum
    // And we are waiting to receive the locked QC in order to commit this
    LockedQC(QuorumView),
    // We are committed to the new quorum
    // And are waiting for the leader to send us the commit QC so we can actually execute the
    CommittedQC(QuorumView),
}
