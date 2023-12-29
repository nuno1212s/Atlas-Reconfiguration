use thiserror::Error;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::error::*;
use atlas_communication::message::Header;
use atlas_core::reconfiguration_protocol::QuorumReconfigurationResponse;
use crate::message::{OperationMessage, QuorumJoinReconfMessages};
use crate::quorum_config::{Node, QuorumObserver};
use crate::quorum_config::network::QuorumConfigNetworkNode;

pub mod quorum_info_op;
pub mod quorum_join_op;
mod propagate_quorum_info;
mod quorum_accept_op;

/// The various responses the operation can return
pub enum OperationResponse {
    /// The operation has been completed, and the replica can move on to the next one
    Completed,
    /// The operation is still in progress, and the replica should wait for the next iteration
    Processing,
    /// We have issued a quorum reconfiguration request and are waiting for the response
    AwaitingResponseProtocol,
}

pub type OpExecID = SeqNo;

/// The trait that represents a possible operation on the current state of the quorum
pub trait Operation: Orderable {

    // The associated name of the operation
    const OP_NAME: &'static str;

    /// The name of this operation
    fn op_name() -> &'static str {
        return Self::OP_NAME;
    }

    /// Can a given node execute this type of operation
    fn can_execute(observer: &Node) -> std::result::Result<(), OperationExecutionCandidateError>;

    /// The ID of the execution instance of this operation
    fn op_exec_id(&self) -> OpExecID;

    /// We want to iterate through this operation (for example to launch the initial requests,
    /// perform re requests on timeouts, etc)
    fn iterate<NT>(&mut self, node: &mut Node, network: &NT) -> Result<OperationResponse>
        where NT: QuorumConfigNetworkNode + 'static;

    /// Handle a message that we have received from another node in the context of this operation
    fn handle_received_message<NT>(&mut self, node: &mut Node, network: &NT, header: Header,
                                   seq_no: SeqNo, message: OperationMessage) -> Result<OperationResponse>
        where NT: QuorumConfigNetworkNode + 'static;

    /// Handle the response of a quorum reconfiguration request sent by this operation
    fn handle_quorum_response<NT>(&mut self, node: &mut Node, network: &NT,
                                  quorum_response: QuorumReconfigurationResponse) -> Result<OperationResponse>
        where NT: QuorumConfigNetworkNode + 'static;

    /// Finish an operation by passing it the node so it can perform the necessary changes
    fn finish<NT>(&mut self, observer: &mut Node, network: &NT) -> Result<()>
        where NT: QuorumConfigNetworkNode + 'static;
}

/// Errors that describe why a given quorum observer cannot execute a given operation
#[derive(Error, Debug)]
pub enum OperationExecutionCandidateError {
    #[error("This operation cannot be executed by a client")]
    ClientCannotExecuteThisOperation,
    #[error("Another operation which conflicts with this one is already ongoing")]
    ConflictingOperations,
    #[error("The requirements for this operation are not met {0}")]
    RequirementsNotMet(String),
    
}