use std::collections::BTreeSet;
use std::sync::Arc;

use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::ReconfigurationNode;

use crate::message::{QuorumEnterRejectionReason, QuorumEnterResponse, QuorumJoinOpMessage, ReconfData};
use crate::quorum_reconfig::node_types::{Node, OpMessageAnalysisResult, OpMessageProcessingResult, QuorumOp};
use crate::quorum_reconfig::QuorumView;

/// Operation of a node attempting to join a quorum
struct NodeQuorumJoinOperation {
    // The sequence number of this operation
    sequence_number: SeqNo,
    // The node that is attempting to join
    joining_node: NodeId,
    // The current quorum of the system, without [joining_node]
    current_quorum: QuorumView,
    // The current phase of the join operation
    current_join_phase: NodeJoinPhase,
    // The votes we have received from other nodes
    received_votes: BTreeSet<NodeId>,
    // The rejections we have received from other nodes
    rejections: Vec<StoredMessage<QuorumEnterRejectionReason>>,
}

enum NodeJoinPhase {
    /// When the node is attempting to join the quorum,
    /// Quorum replicas should broadcast their acceptance (or rejection) to 
    /// all other quorum members.
    /// Once they have 2f+1 votes, it is considered accepted and 
    /// it is passed along to the ordering protocol
    JoiningQuorum(usize),
    /// We are waiting for the order protocol to allow us to join the quorum
    WaitingForOrderProtocol,
    /// The join operation has been completed
    Done,
}

impl NodeQuorumJoinOperation {
    pub fn init_operation(seq_no: SeqNo, joining_node: NodeId, current_quorum: QuorumView) -> Self {
        Self {
            sequence_number: seq_no,
            joining_node,
            current_quorum,
            current_join_phase: NodeJoinPhase::JoiningQuorum(0),
            received_votes: Default::default(),
            rejections: Default::default(),
        }
    }
}

impl Orderable for NodeQuorumJoinOperation {
    fn sequence_number(&self) -> SeqNo {
        self.sequence_number
    }
}

impl QuorumOp for NodeQuorumJoinOperation {
    type OpMessage = QuorumJoinOpMessage;

    fn analyse_message<NT>(node: &Node, nt_node: Arc<NT>, header: &Header, message: &Self::OpMessage) -> Result<OpMessageAnalysisResult> where NT: ReconfigurationNode<ReconfData> + 'static {
        match message {
            QuorumJoinOpMessage::RequestJoinQuorum => {

                //TODO: Launch node quorum join operation on our end

                Ok(OpMessageAnalysisResult::Handled)
            }
            QuorumJoinOpMessage::JoinQuorumResponse(_) => {
                Ok(OpMessageAnalysisResult::Execute)
            }
        }

    }

    fn process_message<NT>(&mut self, nt_node: Arc<NT>, header: Header, message: Self::OpMessage) -> Result<OpMessageProcessingResult> where NT: ReconfigurationNode<ReconfData> + 'static {
        match message {
            QuorumJoinOpMessage::JoinQuorumResponse(join_response) => {
                match join_response {
                    QuorumEnterResponse::Successful(_) => {}
                    QuorumEnterResponse::Rejected(rejection_reason) => {}
                }

            }
            QuorumJoinOpMessage::RequestJoinQuorum => unreachable!()
        }

        Ok(OpMessageProcessingResult::NotDone)
    }

    fn is_done(&self) -> bool {
        match self.current_join_phase {
            NodeJoinPhase::Done => true,
            _ => false
        }
    }

    fn complete(self, node: &mut Node) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Error)]
pub enum NodeJoinError {



}