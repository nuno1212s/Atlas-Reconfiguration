use std::cmp::Ordering;
use std::sync::Arc;
use either::Either;
use log::{info, warn};

use thiserror::Error;

use atlas_common::{Err, quiet_unwrap};
use atlas_common::error::*;
use atlas_common::ordering::{InvalidSeqNo, SeqNo};
use atlas_communication::message::Header;

use crate::message::{CommittedQC, LockedQC, QuorumJoinReconfMessages, QuorumAcceptResponse, QuorumCommitAcceptResponse, QuorumCommitResponse, QuorumJoinResponse, QuorumRejectionReason};
use crate::quorum_config::network::QuorumConfigNetworkNode;
use crate::quorum_config::replica::{get_f_for_n, get_quorum_for_n, QuorumCert, QuorumCertPart};
use crate::quorum_config::QuorumView;


/// Quorum information
/// We are currently a part of the quorum and here is where all of the current data is stored
pub struct QuorumMember {
    // The locked quorum certificate for the quorum we are currently on the process of
    // accepting into the quorum
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


/// The result of handling a quorum reconfiguration message
pub enum QuorumMemberOperationResponse {
    // We have moved to a given quorum
    MovedQuorum(QuorumView),
    Processing,
}


impl QuorumMember {
    pub fn init() -> Self {
        Self {
            locked_qc: None,
            committed_qc: None,
            current_phase: ReplicaPhase::Waiting,
        }
    }

    pub fn iterate<NT>(&mut self, _node: Arc<NT>) -> Result<QuorumMemberOperationResponse> {
        Ok(QuorumMemberOperationResponse::Processing)
    }

    pub fn handle_message<NT>(&mut self, node: &Arc<NT>, current_view: &QuorumView, header: Header, seq_no: SeqNo, message: QuorumJoinReconfMessages)
                              -> Result<QuorumMemberOperationResponse>
        where NT: QuorumConfigNetworkNode + 'static {

        return match message {
            QuorumJoinReconfMessages::RequestJoinQuorum => {
                self.handle_received_join_request(node, seq_no, header, current_view.clone())
            }
            QuorumJoinReconfMessages::CommitQuorum(qc) => {
                self.handle_received_locked_qc(node, header, qc, current_view)
            }
            QuorumJoinReconfMessages::Decided(commit_qc) => {
                self.handle_received_committed_qc(commit_qc, current_view)
            }
            _ => {
                Err!(QuorumMemberError::QuorumMemberReceivingVotes)
            }
        }
    }

    /// Handle us having received a join request from another node
    fn handle_received_join_request<NT>(&mut self, node: &Arc<NT>, seq_no: SeqNo, header: Header, current_view: QuorumView) -> Result<QuorumMemberOperationResponse>
        where NT: QuorumConfigNetworkNode + 'static {

        let response: QuorumJoinResponse;

        match seq_no.index(current_view.sequence_number()) {
            Either::Left(_) | Either::Right(0) => {
                response = QuorumJoinResponse::Rejected(QuorumRejectionReason::SeqNoTooOld);
            }
            Either::Right(1) => {
                if let ReplicaPhase::Waiting = self.current_phase {
                    if current_view.quorum_members().contains(&header.from()) {
                        response = QuorumJoinResponse::Rejected(QuorumRejectionReason::AlreadyAPartOfQuorum);
                    } else {
                        // We are waiting for a new member to try to join our quorum
                        let next_view = current_view.next_with_added_node(header.from(), get_f_for_n(current_view.quorum_members().len() + 1));

                        response = QuorumJoinResponse::Accepted(
                            QuorumAcceptResponse::init(next_view.clone())
                        );

                        self.current_phase = ReplicaPhase::LockedQC(next_view.clone());
                    }
                } else {
                    response = QuorumJoinResponse::Rejected(QuorumRejectionReason::AlreadyAccepting);
                }
            }
            Either::Right(_) => {
                response = QuorumJoinResponse::Rejected(QuorumRejectionReason::SeqNoTooAdvanced);
            }
        }

        quiet_unwrap!(node.send_quorum_config_message(seq_no, QuorumJoinReconfMessages::LockedQuorumResponse(response),
            header.from()));

        // We have already accepted that another node joins the quorum
        Ok(QuorumMemberOperationResponse::Processing)
    }

    /// Handle us having received a locked quorum certificate about a node wanting to enter the quorum.
    /// This means that 2f+1 have locked it and we can now lock and send our commit vote, since we are also
    /// ready to commit that change.
    fn handle_received_locked_qc<NT>(&mut self, node: &Arc<NT>, header: Header, locked_qc: LockedQC, current_quorum: &QuorumView) -> Result<QuorumMemberOperationResponse>
        where NT: QuorumConfigNetworkNode + 'static {
        if let Err(err) = is_valid_qc(&locked_qc, current_quorum) {
            // The QC is not valid
            todo!()
        }

        match &self.current_phase {
            ReplicaPhase::Waiting => {
                self.locked_qc = Some(locked_qc);
                self.current_phase = ReplicaPhase::CommittedQC(locked_qc.quorum().clone());
            }
            ReplicaPhase::LockedQC(currently_locked) => {

                // We have received a locked qc while we have already locked on a given quorum alteration

                // if we don't have any locked QC (which we don't or we would be in the commit phase),
                // then we can just ignore our locked alteration,
                // As the quorum approved one is this (since it has >= 2f+1 votes)

                // We have received a locked quorum certificate, we can now advance to our next
                // phase where we will wait for the next round

                info!("We have received a locked quorum certificate for quorum {:?}, we can now advance to our next phase where we will wait for the next round. \
                We had agreed to the quorum {:?}", locked_qc.quorum(), currently_locked);

                let quorum = locked_qc.quorum().clone();

                self.locked_qc = Some(locked_qc.clone());

                self.current_phase = ReplicaPhase::CommittedQC(quorum.clone());

                let response = QuorumCommitResponse::Accepted(QuorumCommitAcceptResponse::init(quorum, locked_qc));

                quiet_unwrap!(node.send_quorum_config_message(quorum.sequence_number(), QuorumJoinReconfMessages::CommitQuorumResponse(response),
                    header.from()));
            }
            ReplicaPhase::CommittedQC(_) => {
                // We have already received a locked QC since we are in committed QC.
                // So this is irrelevant
                warn!("We have received a locked QC while we are still waiting for the committed QC, handle this");
            }
        }

        Ok(QuorumMemberOperationResponse::Processing)
    }

    /// Handle us having received a committed quorum certificate about a node wanting to enter the quorum.
    /// This means that 2f+1 have committed it and we can execute it.
    fn handle_received_committed_qc(&mut self, committed_qc: CommittedQC, current_quorum: &QuorumView) -> Result<QuorumMemberOperationResponse> {
        if let Err(err) = is_valid_qc(&committed_qc, current_quorum) {
            // The QC is not valid
            todo!()
        }

        let quorum = committed_qc.quorum().clone();

        match &self.current_phase {
            ReplicaPhase::Waiting => {
                info!("We have received a committed qc while we were still in the waiting phase. Accepted it directly and moved to quorum {:?}", quorum);

                self.current_phase = ReplicaPhase::Waiting;
                self.locked_qc = None;
                self.committed_qc = None;

                return Ok(QuorumMemberOperationResponse::MovedQuorum(quorum.clone()));
            }
            ReplicaPhase::LockedQC(_) => {
                info!("We have received a committed qc that was not super seeded by a locked qc");

                todo!("We have received a committed QC while we are still waiting for the locked QC, handle this")
            }
            ReplicaPhase::CommittedQC(committed_to_quorum) => {
                // We have already received a locked QC since we are in committed QC.



                if *committed_to_quorum != *quorum {
                    // Well this is awkward. We have a locked QC for a given quorum but now we have received a
                    // Committed QC for another quorum. How will we handle this?

                    todo!()
                }

                self.current_phase = ReplicaPhase::Waiting;
                self.locked_qc = None;
                self.committed_qc = None;

                return Ok(QuorumMemberOperationResponse::MovedQuorum(quorum.clone()));
            }
        }

        Ok(QuorumMemberOperationResponse::Processing)
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

    match qc.sequence_number().index(current_quorum.sequence_number()) {
        Either::Left(_) | Either::Right(0) => {
            return Err(HandleQCError::QCTooOld(qc.sequence_number(), current_quorum.sequence_number()))
        }
        Either::Right(1) => {}
        Either::Right(_) => {
            // When we have received a quorum certificate that is ahead of us
            // Then we must assume we are the ones in the wrong, since the QC
            // Contains 2f + 1 votes.

            warn!("We have received a quorum certificate that is quite ahead of our current situation. {:?} vs current {:?}",
            qc.sequence_number(), current_quorum.sequence_number());
        }
    }

    let quorum_view = qc.quorum();

    for stored_response in qc.proofs() {
        if *stored_response.view() != *quorum_view {
            return Err(HandleQCError::QCNotAllMatch);
        }
    }

    return Ok(());
}


#[derive(Error, Debug)]
pub enum QuorumMemberError {
    #[error("The quorum member has received votes")]
    QuorumMemberReceivingVotes,
}

#[derive(Error, Debug)]
pub enum HandleQCError {
    #[error("Not enough votes, needed {0}, received {1}")]
    QCNotEnoughVotes(usize, usize),
    #[error("Not all votes on the received certificate match the same quorum view")]
    QCNotAllMatch,
    #[error("The sequence number indicated by the request is too old as our quorum is already ahead {0:?} vs {1:?}")]
    QCTooOld(SeqNo, SeqNo),
}