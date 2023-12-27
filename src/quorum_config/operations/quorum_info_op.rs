use std::collections::{BTreeMap, BTreeSet};
use log::{debug, error, info};
use thiserror::Error;
use atlas_common::crypto::hash::Digest;
use atlas_common::Err;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::SeqNo;
use atlas_communication::message::{Header, StoredMessage};
use crate::message::{QuorumObtainInfoOpMessage, QuorumViewCert};
use crate::quorum_reconfig::QuorumView;

pub struct ObtainQuorumInfoOP {
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

/// The current state of the operation
enum OperationState {
    ReceivingInfo(usize),
    Done,
}

impl ObtainQuorumInfoOP {

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

    pub fn finalize(self) -> atlas_common::error::Result<QuorumView> {
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

/// 
#[derive(Debug, Error)]
pub enum QuorumObtainInfoError {
    #[error("Failed to obtain quorum information: No messages received")]
    FailedNoMessagesReceived,
    #[error("Failed to obtain quorum information: Not enough matching messages, needed {0}, received {1}, digest {2:?}")]
    FailedNotEnoughMatching(usize, usize, Digest),
}