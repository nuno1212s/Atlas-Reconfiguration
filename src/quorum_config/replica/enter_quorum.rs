use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use log::{error, warn};
use thiserror::Error;
use atlas_common::{Err, quiet_unwrap};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::ReconfigurationNode;
use crate::message::{CommittedQC, LockedQC, ParticipatingQuorumMessage, QuorumAcceptResponse, QuorumCommitAcceptResponse, QuorumCommitResponse, QuorumEnterResponse, QuorumJoinResponse, ReconfData, ReconfigurationMessage, ReconfigurationMessageType};
use crate::quorum_config::network::QuorumConfigNetworkNode;
use crate::quorum_config::replica::{EnterQuorumOperationResponse, QuorumCert};
use crate::quorum_reconfig::QuorumView;

/// The information we have about the quorum
/// during our process of entering the quorum
pub struct EnteringQuorum {
    seq_no: SeqNo,
    currently_known_quorum: QuorumView,
    received_votes: BTreeSet<NodeId>,
    threshold: usize,
    // The messages we have received while we are not ready to receive them
    pending_messages: PendingMessages,
    // The rejections we have received from other nodes
    phase: LeaderPhase,
}

/// Messages that are pending since we are not ready to receive them
struct PendingMessages {
    // The commit messages we have received
    pending_commit_message: VecDeque<StoredMessage<ParticipatingQuorumMessage>>,
}

/// The phase of the leader in the quorum reconfiguration protocol
/// In this case, the leader is always the node that is initiating the reconfiguration
///
/// We chose a linear based protocol (instead of a PBFT based one) because we want to
/// prevent the possibility of a replica that is trying to join being able to manipulate this
/// protocol in order to DoS the quorum (which is much easier with PBFT due to the
/// O(n^2) message complexity)
///
/// With this vote and collect based protocol, we assure that the leader is the one with the
/// burden, which makes sense since he is the one that wants to join the protocol
pub enum LeaderPhase {
    // We start by broadcasting our intent to join the quorum
    // Which will yield locked QC responses, where each of the correct
    // Replicas which can accept new quorum nodes will now lock onto the
    // Newest quorum, preventing them from accepting any new requests
    // A lock can be broken in two ways: Either by timing out a given request
    // Or by receiving a lockedQC with 2f+1 members corresponding to the same sequence number
    LockingQC(Vec<StoredMessage<QuorumAcceptResponse>>),
    // After a correct replica receives a correct lockedQC, it will commit that QC
    // And send it's commit confirmation to the leader (us).
    // When we have 2f+1 of these commit confirmations, we can move to the next and
    // Final phase
    // In this phase, we bcast the commit QC (composed of 2f+1 commit certificates).
    // This will make all correct replicas move to the new quorum
    // Any replica in a SeqNo < SeqNo(commitQC) even if it has not locked the quorum previously,
    // Should commit and move to this new quorum as it has received proof that the rest of the
    // Quorum has already done so
    CommittingQC(Vec<StoredMessage<QuorumCommitAcceptResponse>>),

    Done,
}

/// The responses possible while executing this operation
pub enum EnterQuorumOperationResponse {
    // We entered the quorum
    EnterQuorum(QuorumView),
    Processing,
}

impl Orderable for EnteringQuorum {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}

impl EnteringQuorum {
    pub fn init_operation(current_quorum: QuorumView, threshold: usize) -> Self {
        Self {
            seq_no: current_quorum.sequence_number(),
            currently_known_quorum: current_quorum,
            received_votes: Default::default(),
            threshold,
            pending_messages: Default::default(),
            phase: LeaderPhase::LockingQC(Default::default()),
        }
    }

    /// Handles a message we have received from another node
    pub fn handle_message<NT>(&mut self, node: &Arc<NT>, header: Header, message: ParticipatingQuorumMessage) -> Result<EnterQuorumOperationResponse>
        where NT: QuorumConfigNetworkNode + 'static {
        match &mut self.phase {
            LeaderPhase::LockingQC(received_approvals) => {
                match message {
                    ParticipatingQuorumMessage::RequestJoinQuorum => {}
                    ParticipatingQuorumMessage::LockedQuorumResponse(lock_vote) => {
                        match lock_vote {
                            QuorumJoinResponse::Accepted(vote) => {
                                received_approvals.push(StoredMessage::new(header, vote));
                            }
                            QuorumJoinResponse::Rejected(_) => {
                                todo!("Handle rejection of join request")
                            }
                        }
                    }
                    _ => self.pending_messages.push_pending_message(StoredMessage::new(header, message))
                }

                if received_approvals.len() >= self.threshold {
                    let phase = std::mem::replace(&mut self.phase, LeaderPhase::CommittingQC(Default::default()));

                    let locked_qc = phase.unwrap_locked_qc();

                    let qc = LockedQC::new(locked_qc);

                    quiet_unwrap!(node.broadcast_quorum_message(self.sequence_number(), ParticipatingQuorumMessage::CommitQuorum(qc),
                        self.currently_known_quorum.quorum_members().clone().into_iter()));
                }
            }
            LeaderPhase::CommittingQC(received_commit_votes) => {
                match message {
                    ParticipatingQuorumMessage::CommitQuorumResponse(commit_vote) => {
                        match commit_vote {
                            QuorumCommitResponse::Accepted(vote) => {
                                received_commit_votes.push(StoredMessage::new(header, vote));
                            }
                            QuorumCommitResponse::Rejected(_) => {
                                todo!("Handle rejection of commit request")
                            }
                        }
                    }
                    _ => self.pending_messages.pending_commit_message(StoredMessage::new(header, message))
                }

                if received_commit_votes.len() >= self.threshold {
                    let phase = std::mem::replace(&mut self.phase, LeaderPhase::Done);

                    let commit_qc = phase.unwrap_commit_qc();

                    let commit_qc = CommittedQC::new(commit_qc);

                    let quorum = commit_qc.quorum().clone();

                    quiet_unwrap!(node.broadcast_quorum_message(self.sequence_number(), ParticipatingQuorumMessage::Decided(commit_qc),
                        self.currently_known_quorum.quorum_members().clone().into_iter()));

                    return Ok(EnterQuorumOperationResponse::EnterQuorum(quorum));
                }
            }
            LeaderPhase::Done => {
                warn!("Received message while we are done with the protocol");
            }
        }

        Ok(EnterQuorumOperationResponse::Processing)
    }

    fn is_done(&self) -> bool {
        matches!(self.phase, LeaderPhase::Done)
    }
}

#[derive(Error, Debug)]
pub enum EnteringQuorumError {
    CannotApproveJoinRequestsWhileJoining
}

impl PendingMessages {
    pub fn new() -> Self {
        Self {
            pending_commit_message: Default::default(),
        }
    }

    pub fn push_pending_message(&mut self, message: StoredMessage<ParticipatingQuorumMessage>) {
        match message.message() {
            ParticipatingQuorumMessage::CommitQuorumResponse(_) => {
                self.pending_commit_message.push_back(message);
            }
            _ => {
                error!("Received unexpected message while not ready to receive it. (Type does not match what we are expecting)");
            }
        }
    }
}

impl Default for PendingMessages {
    fn default() -> Self {
        Self::new()
    }
}

impl LeaderPhase {
    fn unwrap_locked_qc(self) -> Vec<StoredMessage<QuorumAcceptResponse>> {
        match self {
            LeaderPhase::LockingQC(locked_qc) => locked_qc,
            _ => unreachable!("Called unwrap_locked_qc on a non-locking phase")
        }
    }

    fn unwrap_commit_qc(self) -> Vec<StoredMessage<QuorumCommitAcceptResponse>> {
        match self {
            LeaderPhase::CommittingQC(commit_qc) => commit_qc,
            _ => unreachable!("Called unwrap_commit_qc on a non-committing phase")
        }
    }
}