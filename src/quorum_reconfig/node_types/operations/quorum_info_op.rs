use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use log::{debug, error, info};
use thiserror::Error;
use atlas_common::crypto::hash::Digest;
use atlas_common::Err;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::error::*;
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::ReconfigurationNode;
use crate::message::{QuorumObtainInfoOpMessage, QuorumViewCert, ReconfData};
use crate::quorum_reconfig::node_types::{Node, OpMessageAnalysisResult, QuorumOp};
use crate::quorum_reconfig::QuorumView;

/// Operation of a node attempting to obtain quorum information
/// from the already existing quorum
struct QuorumObtainInformationOperation {
    // The sequence number of this operation
    sequence_number: SeqNo,
    // The current known quorum
    current_known_quorum: Vec<NodeId>,
    // The amount of nodes we have to receive messages from
    // To consider it done
    threshold: usize,
    // The responses that we have received
    received: BTreeSet<NodeId>,
    // The quorum views we have received
    quorum_views: BTreeMap<Digest, Vec<QuorumViewCert>>,
    // The current state of the operation we are currently performing
    state: OperationState,
}

enum OperationState {
    ReceivingInfo(usize),
    Done,
}

impl QuorumObtainInformationOperation {
    pub fn initialize(seq_no: SeqNo, threshold: usize, current_known_quorum: Vec<NodeId>) -> Self {

        //TODO: Broadcast the quorum information request to the currently known quorum

        Self {
            sequence_number: seq_no,
            current_known_quorum,
            threshold,
            received: BTreeSet::new(),
            quorum_views: BTreeMap::new(),
            state: OperationState::ReceivingInfo(0),
        }
    }

    pub fn handle_message(&mut self, header: Header, message: QuorumObtainInfoOpMessage) {
        match message {
            QuorumObtainInfoOpMessage::RequestInformationMessage => {
                unreachable!("Received request information message while we are the ones requesting information")
            }
            QuorumObtainInfoOpMessage::QuorumInformationResponse(quorum) => {
                match &mut self.state {
                    OperationState::ReceivingInfo(received) if self.received.insert(header.from()) => {
                        *received += 1;

                        let digest = header.digest().clone();

                        self.quorum_views.entry(digest).or_insert_with(Vec::new).push(StoredMessage::new(header, quorum));

                        if *received >= self.threshold {
                            if self.quorum_views.values().filter(|certs| certs.len() >= self.threshold).count() >= self.threshold {
                                self.state = OperationState::Done;
                            } else {
                                info!("Received enough responses, but not enough matching quorum views, waiting for more");
                            }
                        }
                    }
                    OperationState::ReceivingInfo(_) => {
                        error!("Received duplicate message from node {:?}", header.from());
                    }
                    OperationState::Done => debug!("Received message from node {:?} after operation was done", header.from())
                }
            }
        }
    }

    pub fn finalize(self) -> Result<QuorumView> {
        let mut received_messages = Vec::new();

        for (message_digests, messages) in self.quorum_views.iter() {
            received_messages.push((message_digests.clone(), messages.clone()));
        }

        received_messages.sort_by(|(_, a), (_, b)| {
            a.len().cmp(&b.len()).reverse()
        });

        debug!("Processed received messages: {:?}", received_messages);

        if let Some((quorum_digest, quorum_certs)) = received_messages.first() {
            if quorum_certs.len() >= self.threshold {
                let view = quorum_certs.first().unwrap().message().clone();

                Ok(view)
            } else {
                Err!(QuorumObtainInfoError::FailedNotEnoughMatching(self.threshold, quorum_certs.len(), quorum_digest.clone()))
            }
        } else {
            Err!(QuorumObtainInfoError::FailedNoMessagesReceived)
        }
    }
}

impl Orderable for QuorumObtainInformationOperation {
    fn sequence_number(&self) -> SeqNo {
        self.sequence_number
    }
}

impl QuorumOp for QuorumObtainInformationOperation {
    type OpMessage = QuorumObtainInfoOpMessage;

    fn analyse_message<NT>(node: &Node, nt_node: Arc<NT>, header: &Header, message: &Self::OpMessage) -> Result<OpMessageAnalysisResult>
        where NT: ReconfigurationNode<ReconfData> + 'static {
        match message {
            QuorumObtainInfoOpMessage::RequestInformationMessage => {
                let message = QuorumObtainInfoOpMessage::QuorumInformationResponse(node.quorum_view.view());

                //nt_node.send_reconfig_message(message, header.from());
            }
            _ => Ok(OpMessageAnalysisResult::Execute)
        }

        Ok(OpMessageAnalysisResult::Handled)
    }

    fn process_message<NT>(&mut self, _nt_node: Arc<NT>, header: Header, message: Self::OpMessage) -> Result<()>
        where NT: ReconfigurationNode<ReconfData> + 'static {

        self.handle_message(header, message);

        Ok(())
    }

    fn is_done(&self) -> bool {
        match &self.state {
            OperationState::Done => true,
            _ => false
        }
    }

    fn complete(self, node: &mut Node) -> Result<()> {
        let quorum_view = self.finalize()?;

        node.quorum_view.install_view(quorum_view);

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum QuorumObtainInfoError {
    #[error("Failed to obtain quorum information: No messages received")]
    FailedNoMessagesReceived,
    #[error("Failed to obtain quorum information: Not enough matching messages, needed {0}, received {1}, digest {2:?}")]
    FailedNotEnoughMatching(usize, usize, Digest),
}