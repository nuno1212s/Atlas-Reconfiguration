use std::sync::Arc;
use thiserror::Error;

use atlas_common::Err;
use atlas_common::error::*;
use atlas_common::ordering::Orderable;
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::ReconfigurationNode;

use crate::message::{CommittedQC, LockedQC, ParticipatingQuorumMessage, QuorumAcceptResponse, QuorumCommitAcceptResponse, ReconfData};
use crate::quorum_config::replica::enter_quorum::EnteringQuorum;
use crate::quorum_config::replica::quorum_member::{QuorumMember, ReplicaPhase};
use crate::quorum_reconfig::node_types::QuorumViewer;
use crate::quorum_reconfig::QuorumView;

mod enter_quorum;
mod quorum_member;

/// This a struct encapsulating all of the logic of a replica participating in the quorum
pub struct QuorumParticipator {
    viewer: QuorumViewer,
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
    pub fn handle_message<NT>(&mut self, node: &Arc<NT>, header: Header, message: ParticipatingQuorumMessage) -> Result<()>
        where NT: ReconfigurationNode<ReconfData> + 'static {
        match &mut self.state {
            CurrentState::AFK => {
                // We have nothing to do here
                return Err!(ParticipatorHandleMessageError::CurrentlyAfk);
            }
            CurrentState::OutOfQuorum(info) => {
                let x = info.handle_message(node, header, message)?;
                
                
            }
            CurrentState::QuorumMember(member) => {
                member.handle_message(&self.viewer.view(), node, header, message)?;
            }
        }

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum ParticipatorHandleMessageError {
    #[error("We are current away from the quorum and therefore cannot process this message")]
    CurrentlyAfk
}

pub trait QuorumCert : Orderable {
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