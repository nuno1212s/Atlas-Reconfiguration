use std::sync::Arc;
use thiserror::Error;

use atlas_common::Err;
use atlas_common::error::*;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::ReconfigurationNode;

use crate::message::{CommittedQC, LockedQC, QuorumJoinReconfMessages, QuorumAcceptResponse, QuorumCommitAcceptResponse, ReconfData};
use crate::quorum_config::{QuorumObserver, QuorumView};
use crate::quorum_config::replica::enter_quorum::{EnteringQuorum, EnterQuorumOperationResponse};
use crate::quorum_config::replica::quorum_member::{QuorumMember, QuorumMemberOperationResponse, ReplicaPhase};
use crate::QuorumProtocolResponse;


mod enter_quorum;
mod quorum_member;

/// This a struct encapsulating all of the logic of a replica participating in the quorum
pub struct QuorumParticipator {
    viewer: QuorumObserver,
    state: CurrentState,
}

/// The current state of the replica
pub enum CurrentState {
    AFK,
    // We are out of the quorum, but are in the process of being accepted
    OutOfQuorum(EnteringQuorum),
    // We are currently a member of the quorum
    QuorumMember(QuorumMember),
}


impl QuorumParticipator {
    pub fn initialize(observer: QuorumObserver) -> Self {
        Self {
            viewer: observer,
            state: CurrentState::AFK,
        }
    }

    pub fn iterate<NT>(&mut self, node: Arc<NT>) -> Result<QuorumProtocolResponse> {
        match &mut self.state {
            CurrentState::AFK => {
                // We have nothing to do here
                Err!(ParticipatorHandleMessageError::CurrentlyAfk)
            }
            CurrentState::OutOfQuorum(entering) => {
                match entering.iterate(node)? {
                    EnterQuorumOperationResponse::EnterQuorum(quorum) => {
                        self.handle_quorum_entered(quorum)
                    }
                    EnterQuorumOperationResponse::Processing => {
                        Ok(QuorumProtocolResponse::Nil)
                    }
                }
            }
            CurrentState::QuorumMember(quorum_member) => {
                match quorum_member.iterate(node)? {
                    QuorumMemberOperationResponse::MovedQuorum(quorum) => {
                        self.handle_quorum_altered(quorum)
                    }
                    QuorumMemberOperationResponse::Processing => {
                        Ok(QuorumProtocolResponse::Nil)
                    }
                }
            }
        }
    }

    fn handle_quorum_entered(&mut self, quorum: QuorumView) -> Result<QuorumProtocolResponse> {
        self.state = CurrentState::QuorumMember(QuorumMember::init());

        self.viewer.install_quorum_view(quorum);

        return Ok(QuorumProtocolResponse::Done);
    }

    fn handle_quorum_altered(&mut self, quorum: QuorumView) -> Result<QuorumProtocolResponse> {

        self.viewer.install_quorum_view(quorum);

        Ok(QuorumProtocolResponse::Nil)
    }

    pub fn handle_message<NT>(&mut self, node: &Arc<NT>, header: Header, seq_no: SeqNo, message: QuorumJoinReconfMessages) -> Result<QuorumProtocolResponse>
        where NT: ReconfigurationNode<ReconfData> + 'static {
        return match &mut self.state {
            CurrentState::AFK => {
                // We have nothing to do here
                return Err!(ParticipatorHandleMessageError::CurrentlyAfk);
            }
            CurrentState::OutOfQuorum(info) => {
                match info.handle_message(node, header, message)? {
                    EnterQuorumOperationResponse::EnterQuorum(quorum) => {
                        self.handle_quorum_entered(quorum)
                    }
                    EnterQuorumOperationResponse::Processing => {
                        Ok(QuorumProtocolResponse::Nil)
                    }
                }
            }
            CurrentState::QuorumMember(member) => {
                match member.handle_message(node, &self.viewer.current_view(), header, seq_no, message)? {
                    QuorumMemberOperationResponse::MovedQuorum(quorum) => {
                        self.handle_quorum_altered(quorum)
                    }
                    QuorumMemberOperationResponse::Processing => {
                        Ok(QuorumProtocolResponse::Nil)
                    }
                }
            }
        };
    }
}

#[derive(Error, Debug)]
pub enum ParticipatorHandleMessageError {
    #[error("We are current away from the quorum and therefore cannot process this message")]
    CurrentlyAfk
}

pub trait QuorumCert: Orderable {
    type IndividualType: QuorumCertPart;

    fn quorum(&self) -> &QuorumView;

    fn proofs(&self) -> &[Self::IndividualType];
}

pub trait QuorumCertPart {
    fn view(&self) -> &QuorumView;
}

/// Get the amount of tolerated faults for a network of n nodes
/// This returns the amount of faults that can be tolerated
pub fn get_f_for_n(n: usize) -> usize {
    (n - 1) / 3
}

/// Get the amount of nodes required to form a quorum in a BFT network which tolerates [f] faults
pub fn get_quorum_for_f(f: usize) -> usize {
    2 * f + 1
}

pub fn get_quorum_for_n(n: usize) -> usize {
    get_quorum_for_f(get_f_for_n(n))
}


impl Orderable for LockedQC {
    fn sequence_number(&self) -> SeqNo {
        self.quorum().sequence_number()
    }
}

impl QuorumCert for LockedQC {
    type IndividualType = StoredMessage<QuorumAcceptResponse>;

    fn quorum(&self) -> &QuorumView {
        self.quorum()
    }

    fn proofs(&self) -> &[Self::IndividualType] {
        self.proofs().as_slice()
    }
}

impl QuorumCertPart for StoredMessage<QuorumAcceptResponse> {
    fn view(&self) -> &QuorumView {
        self.message().view()
    }
}

impl Orderable for CommittedQC {
    fn sequence_number(&self) -> SeqNo {
        self.quorum().sequence_number()
    }
}

impl QuorumCert for CommittedQC {
    type IndividualType = StoredMessage<QuorumCommitAcceptResponse>;

    fn quorum(&self) -> &QuorumView {
        self.quorum()
    }

    fn proofs(&self) -> &[Self::IndividualType] {
        self.proofs().as_slice()
    }
}

impl QuorumCertPart for StoredMessage<QuorumCommitAcceptResponse> {
    fn view(&self) -> &QuorumView {
        self.message().view()
    }
}