use std::collections::{BTreeSet, VecDeque};
use std::future::Pending;
use log::{error, info, warn};
use thiserror::Error;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::quiet_unwrap;
use atlas_communication::message::{Header, StoredMessage};
use atlas_core::reconfiguration_protocol::QuorumReconfigurationResponse;
use crate::message::{CommittedQC, LockedQC, OperationMessage, QuorumAcceptResponse, QuorumCommitAcceptResponse, QuorumCommitResponse, QuorumJoinReconfMessages, QuorumJoinResponse};
use crate::quorum_config::network::QuorumConfigNetworkNode;
use crate::quorum_config::Node;
use crate::quorum_config::operations::{Operation, OperationExecutionCandidateError, OperationResponse, OpExecID};
use crate::quorum_config::operations::quorum_info_op::ObtainQuorumInfoOP;
use crate::quorum_config::replica::QuorumCert;

/// The operation to enter the quorum
pub struct EnterQuorumOperation {
    // The execution ID of this operation
    op_seq: OpExecID,
    // The votes that we have received so far
    received_votes: BTreeSet<NodeId>,
    // The threshold of votes we need to receive
    threshold: usize,
    // The messages that we have to analyse once we are ready to
    pending_messages: PendingMessages,

    // The phase of the currently executing operation
    phase: OperationPhase,
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
pub enum OperationPhase {
    Waiting,
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

/// Messages that are pending since we are not ready to receive them
struct PendingMessages {
    // The commit messages we have received
    pending_commit_message: VecDeque<StoredMessage<QuorumJoinReconfMessages>>,
}

impl Orderable for EnterQuorumOperation {
    fn sequence_number(&self) -> SeqNo {
        self.op_seq
    }
}

impl EnterQuorumOperation {
    fn unwrap_operation_message(message: OperationMessage) -> QuorumJoinReconfMessages {
        match message {
            OperationMessage::QuorumReconfiguration(message) => message,
            _ => unreachable!("Received non quorum join reconf message in enter quorum operation")
        }
    }
}

impl Operation for EnterQuorumOperation {
    const OP_NAME: &'static str = "ENTER_QUORUM";

    fn can_execute(observer: &Node) -> std::result::Result<(), OperationExecutionCandidateError> {
        let last_execution = observer.data().get(ObtainQuorumInfoOP::OP_NAME, ObtainQuorumInfoOP::LAST_EXEC);

        if let None = last_execution {
            return Err(OperationExecutionCandidateError::RequirementsNotMet(format!("{}", JoinExecErr::QuorumInformationMustBeObtained)));
        }

        Ok(())
    }

    fn op_exec_id(&self) -> OpExecID {
        self.op_seq
    }

    fn iterate<NT>(&mut self, node: &mut Node, network: &NT) -> Result<OperationResponse>
        where NT: QuorumConfigNetworkNode + 'static {
        if let OperationPhase::Waiting = &self.phase {
            let current_view = node.observer.current_view();

            info!("Broadcasting enter quorum request to known quorum: {:?}", current_view);

            self.phase = OperationPhase::CommittingQC(Default::default());

            network.broadcast_quorum_message(self.sequence_number().next(), QuorumJoinReconfMessages::RequestJoinQuorum,
                                             current_view.quorum_members().clone().into_iter())?;
        }

        Ok(OperationResponse::Processing)
    }

    fn handle_received_message<NT>(&mut self, node: &mut Node, network: &NT, header: Header, seq_no: SeqNo, message: OperationMessage) -> Result<OperationResponse>
        where NT: QuorumConfigNetworkNode + 'static {

        let message = Self::unwrap_operation_message(message);

        let current_view = node.observer.current_view();

        match &mut self.phase {
            OperationPhase::Waiting => {}
            OperationPhase::LockingQC(received_approvals) => {
                match message {
                    QuorumJoinReconfMessages::RequestJoinQuorum => {}
                    QuorumJoinReconfMessages::LockedQuorumResponse(lock_vote) => {
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
                    let phase = std::mem::replace(&mut self.phase, OperationPhase::CommittingQC(Default::default()));

                    let locked_qc = phase.unwrap_locked_qc();

                    let qc = LockedQC::new(locked_qc);

                    quiet_unwrap!(network.broadcast_quorum_message(self.sequence_number(), QuorumJoinReconfMessages::CommitQuorum(qc),
                        current_view.quorum_members().clone().into_iter()));
                }
            }
            OperationPhase::CommittingQC(received_commit_votes) => {
                match message {
                    QuorumJoinReconfMessages::CommitQuorumResponse(commit_vote) => {
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
                    let phase = std::mem::replace(&mut self.phase, OperationPhase::Done);

                    let commit_qc = phase.unwrap_commit_qc();

                    let commit_qc = CommittedQC::new(commit_qc);

                    let quorum = commit_qc.quorum().clone();

                    quiet_unwrap!(node.broadcast_quorum_message(self.sequence_number(), QuorumJoinReconfMessages::Decided(commit_qc),
                        current_view.quorum_members().clone().into_iter()));

                    return Ok(crate::quorum_config::replica::enter_quorum::EnterQuorumOperationResponse::EnterQuorum(quorum));
                }
            }
            OperationPhase::Done => {
                warn!("Received message while we are done with the protocol");
            }
        }

        Ok(OperationResponse::Processing)
    }

    fn handle_quorum_response<NT>(&mut self, node: &mut Node, network: &NT, quorum_response: QuorumReconfigurationResponse) -> Result<OperationResponse>
        where NT: QuorumConfigNetworkNode + 'static {
        todo!()
    }

    fn finish<NT>(&mut self, observer: &mut Node, network: &NT) -> Result<()> where NT: QuorumConfigNetworkNode + 'static {
        todo!()
    }
}

impl PendingMessages {
    pub fn new() -> Self {
        Self {
            pending_commit_message: Default::default(),
        }
    }

    pub fn push_pending_message(&mut self, message: StoredMessage<QuorumJoinReconfMessages>) {
        match message.message() {
            QuorumJoinReconfMessages::CommitQuorumResponse(_) => {
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

#[derive(Error, Debug)]
pub enum JoinExecErr {
    #[error("The Obtain quorum information operation is required for this operation to execute")]
    QuorumInformationMustBeObtained,
}