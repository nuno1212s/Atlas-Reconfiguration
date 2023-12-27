use thiserror::Error;
use crate::message::{CommittedQC, LockedQC, ParticipatingQuorumMessage};
use crate::quorum_reconfig::QuorumView;
use atlas_common::error::*;
use atlas_communication::message::Header;
use crate::quorum_config::replica::{get_quorum_for_n, QuorumCert, QuorumCertPart};
use crate::quorum_reconfig::node_types::QuorumViewer;

/// Quorum information
pub struct QuorumMember {
    locked_qc: Option<LockedQC>,
    committed_qc: Option<CommittedQC>,

    current_phase: ReplicaPhase,
}

/// The phase of a replica in the quorum reconfiguration protocol
pub enum ReplicaPhase {
    // We are waiting for a new member to try to join our quorum
    Waiting,
    // The new member has requested to join our quorum and we have now locked onto the new quorum
    // And we are waiting to receive the locked QC in order to commit this
    LockedQC(QuorumView),
    // We are committed to the new quorum
    // And are waiting for the leader to send us the commit QC so we can actually execute the
    CommittedQC(QuorumView),
}


impl QuorumMember {
    pub fn handle_message(&mut self, current_view: &QuorumView, header: Header, message: ParticipatingQuorumMessage) -> Result<QuorumOperationResponse> {
        match &mut self.current_phase {
            ReplicaPhase::Waiting => {
                match message {
                    ParticipatingQuorumMessage::RequestJoinQuorum => {}
                    ParticipatingQuorumMessage::LockedQuorumResponse(_) => {}
                    ParticipatingQuorumMessage::CommitQuorum(locked_qc) => {}
                    ParticipatingQuorumMessage::CommitQuorumResponse(_) => {}
                    ParticipatingQuorumMessage::Decided(committed_qc) => {}
                }
            }
            ReplicaPhase::LockedQC(locked_view) => {
                match message {
                    ParticipatingQuorumMessage::CommitQuorum(quorum_cert) => {}
                    _ => todo!()
                }
            }
            ReplicaPhase::CommittedQC(_) => {}
        }

        Ok(QuorumOperationResponse::Processing)
    }

    /// Handle us having received a locked quorum certificate about a node wanting to enter the quorum.
    /// This means that 2f+1 have locked it and we can now lock and send our commit vote, since we are also
    /// ready to commit that change.
    fn handle_received_locked_qc(&mut self, locked_qc: LockedQC, current_quorum: &QuorumView) -> Result<()> {
        if let Err(err) = is_valid_qc(&locked_qc, current_quorum) {
            // The QC is not valid
            todo!()
        }

        let quorum = locked_qc.quorum();

        match &self.current_phase {
            ReplicaPhase::Waiting => {
                self.current_phase = ReplicaPhase::CommittedQC(quorum.clone());
            }
            ReplicaPhase::LockedQC(currently_locked) => {

                // We have received a locked qc while we have already locked on a given quorum alteration

                // if we don't have any locked QC (which we don't or we would be in the commit phase),
                // then we can just ignore our locked alteration,
                // As the quorum approved one is this (since it has >= 2f+1 votes)

                // We have received a locked quorum certificate, we can now advance to our next
                // phase where we will wait for the next round
                self.locked_qc = Some(locked_qc);

                self.current_phase = ReplicaPhase::CommittedQC(quorum.clone());

                //TODO: Send commit vote.
            }
            ReplicaPhase::CommittedQC(_) => {
                // We have already received a locked QC since we are in committed QC.
            }
        }

        Ok(())
    }

    /// Handle us having received a committed quorum certificate about a node wanting to enter the quorum.
    /// This means that 2f+1 have committed it and we can execute it.
    fn handle_received_committed_qc(&mut self, viewer: &QuorumViewer, committed_qc: CommittedQC, current_quorum: &QuorumView) -> Result<()> {
        if let Err(err) = is_valid_qc(&committed_qc, current_quorum) {
            // The QC is not valid
            todo!()
        }

        let quorum = committed_qc.quorum();

        match &self.current_phase {
            ReplicaPhase::Waiting => {
                self.current_phase = ReplicaPhase::CommittedQC(quorum.clone());
            }
            ReplicaPhase::LockedQC(_) => {
                todo!("We have received a committed QC while we are still waiting for the locked QC, handle this")
            }
            ReplicaPhase::CommittedQC(committed_to_quorum) => {
                // We have already received a locked QC since we are in committed QC.

                if *committed_to_quorum != *quorum {
                    // Well this is awkward. We have a locked QC for a given quorum but now we have received a
                    // Committed QC for another quorum. How will we handle this?

                    todo!()
                }

                viewer.install_view(quorum.clone());

                self.current_phase = ReplicaPhase::Waiting;
                self.locked_qc = None;
                self.committed_qc = None;
            }
        }

        Ok(())
    }
}


/// Check if the given quorum certificate is valid
fn is_valid_qc<QC>(qc: &QC, current_quorum: &QuorumView) -> std::result::Result<(), HandleQCError>
    where QC: QuorumCert {
    let needed_quorum = get_quorum_for_n(current_quorum.quorum_members().len());

    if qc.proofs().len() < needed_quorum {
        // The QC does not have enough votes to be valid
        return Err(HandleQCError::QCNotEnoughVotes(needed_quorum, qc.proofs().len()));
    }

    let quorum_view = qc.quorum();

    for stored_response in qc.proofs() {
        if *stored_response.view() != *quorum_view {
            return Err(HandleQCError::QCNotAllMatch);
        }
    }

    return Ok(());
}

pub enum QuorumOperationResponse {
    MovedQuorum(QuorumView),
    Processing,

}

#[derive(Error, Debug)]
pub enum QuorumMemberError {}

#[derive(Error, Debug)]
pub enum HandleQCError {
    #[error("Not enough votes, needed {0}, received {1}")]
    QCNotEnoughVotes(usize, usize),
    #[error("Not all votes on the received certificate match the same quorum view")]
    QCNotAllMatch,
}