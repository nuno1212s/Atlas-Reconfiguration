use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use log::{debug, error, info};
use atlas_common::channel::ChannelSyncTx;

use atlas_common::crypto::hash::Digest;
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::ordering::SeqNo;
use atlas_communication::message::Header;

use atlas_communication::reconfiguration_node::ReconfigurationNode;
use atlas_core::reconfiguration_protocol::{QuorumReconfigurationMessage, QuorumUpdateMessage};
use atlas_core::timeouts::Timeouts;

use crate::{GeneralNodeInfo, SeqNoGen, TIMEOUT_DUR};
use crate::message::{QuorumReconfigMessage, QuorumViewCert, ReconfData, ReconfigurationMessage, ReconfigurationMessageType};
use crate::quorum_reconfig::node_types::QuorumNodeResponse;
use crate::quorum_reconfig::QuorumView;

enum ClientState {
    /// We are initializing our state
    Init,
    /// We are receiving quorum information from the network
    Initializing(usize, BTreeSet<NodeId>, BTreeMap<Digest, Vec<QuorumViewCert>>),
    /// We are up to date with the quorum members
    Stable,

}

///
pub(crate) struct ClientQuorumView {
    current_state: ClientState,
    current_quorum_view: Arc<RwLock<QuorumView>>,

    /// The set of messages that we have received that are part of the current quorum view
    /// That agree on the current
    quorum_view_certificate: Vec<QuorumViewCert>,

    /// Channel to send update messages to the client
    channel_message: ChannelSyncTx<QuorumUpdateMessage>,
}

impl ClientQuorumView {
    pub fn new(quorum_view: Arc<RwLock<QuorumView>>, message: ChannelSyncTx<QuorumUpdateMessage>, min_stable_quorum: usize) -> Self {
        ClientQuorumView {
            current_state: ClientState::Init,
            current_quorum_view: quorum_view,
            quorum_view_certificate: vec![],
            channel_message: message,
        }
    }

    pub fn iterate<NT>(&mut self, seq_no: &mut SeqNoGen, node: &GeneralNodeInfo, network_node: &Arc<NT>, timeouts: &Timeouts) -> QuorumNodeResponse
        where NT: ReconfigurationNode<ReconfData> {
        match &mut self.current_state {
            ClientState::Init => {
                let reconf_message = QuorumReconfigMessage::NetworkViewStateRequest;

                let known_nodes: Vec<_> = node.network_view.known_nodes_and_types().iter().filter_map(|(node, node_type)| {
                    match node_type {
                        NodeType::Replica => Some(*node),
                        NodeType::Client => None,
                    }
                }).collect();

                let contacted_nodes = known_nodes.len();

                info!("{:?} // Broadcasting view state request to known nodes {:?}", node.network_view.node_id(), known_nodes);

                let message = ReconfigurationMessage::new(seq_no.next_seq(), ReconfigurationMessageType::QuorumReconfig(reconf_message));

                let _ = network_node.broadcast_reconfig_message(message, known_nodes.into_iter());

                timeouts.timeout_reconfig_request(TIMEOUT_DUR, (contacted_nodes / 2 + 1) as u32, seq_no.curr_seq());

                self.current_state = ClientState::Initializing(contacted_nodes, Default::default(), Default::default());

                QuorumNodeResponse::Nil
            }
            _ => {
                //Nothing to do here
                QuorumNodeResponse::Nil
            }
        }
    }

    /// Handle a view state message being received
    pub fn handle_view_state_message<NT>(&mut self, seq_no: &SeqNoGen, node: &GeneralNodeInfo, network_node: &Arc<NT>, quorum_view_state: QuorumViewCert)
                                         -> QuorumNodeResponse
        where NT: ReconfigurationNode<ReconfData> + 'static {
        match &mut self.current_state {
            ClientState::Init => {
                //TODO: Maybe require at least more than one message to be received before we change state?
            }
            ClientState::Initializing(sent_messages, received, received_message) => {
                // We are still initializing, so we need to add this message to the list of received messages

                let sender = quorum_view_state.header().from();
                let digest = quorum_view_state.header().digest().clone();

                if received.insert(sender) {
                    let entry = received_message.entry(digest)
                        .or_insert(Default::default());

                    entry.push(quorum_view_state.clone());
                } else {
                    error!("Received duplicate message from node {:?} with digest {:?}",
                           sender, digest);
                }

                let needed_messages = *sent_messages / 2 + 1;

                if received.len() >= needed_messages {
                    // We have received all of the messages that we are going to receive, so we can now
                    // determine the current quorum view

                    let mut received_messages = Vec::new();

                    for (message_digests, messages) in received_message.iter() {
                        received_messages.push((message_digests.clone(), messages.clone()));
                    }

                    received_messages.sort_by(|(_, a), (_, b)| {
                        a.len().cmp(&b.len()).reverse()
                    });

                    if let Some((quorum_digest, quorum_certs)) = received_messages.first() {
                        if quorum_certs.len() >= needed_messages {
                            let nodes = {
                                let mut write_guard = self.current_quorum_view.write().unwrap();

                                *write_guard = quorum_certs.first().unwrap().message().clone();

                                write_guard.quorum_members.clone()
                            };

                            self.quorum_view_certificate = quorum_certs.clone();

                            self.channel_message.send(QuorumUpdateMessage::UpdatedQuorumView(nodes));

                            self.current_state = ClientState::Stable;

                            return QuorumNodeResponse::Done;
                        }
                    } else {
                        error!("Received no messages from any nodes");
                    }
                }
            }
            ClientState::Stable => {
                // We are already stable, so we don't need to do anything
            }
        }

        QuorumNodeResponse::Nil
    }

    pub(crate) fn handle_view_state_request<NT>(&self, seq_gen: &mut SeqNoGen, node: &GeneralNodeInfo, network_node: &Arc<NT>, header: Header, seq: SeqNo) -> QuorumNodeResponse
        where NT: 'static + ReconfigurationNode<ReconfData> {
        let quorum_view = self.current_quorum_view.read().unwrap().clone();

        let resp = ReconfigurationMessage::new(seq, ReconfigurationMessageType::QuorumReconfig(QuorumReconfigMessage::NetworkViewState(quorum_view)));

        network_node.send_reconfig_message(resp, header.from());

        QuorumNodeResponse::Nil
    }


    /// Handle a timeout received from the timeout layer
    pub fn handle_timeout<NT>(&mut self, seq_no: &mut SeqNoGen, node: &GeneralNodeInfo, network_node: &Arc<NT>, timeouts: &Timeouts) -> QuorumNodeResponse
        where NT: 'static + ReconfigurationNode<ReconfData> {
        match &mut self.current_state {
            ClientState::Initializing(contacted, received, certs) => {
                let reconf_message = QuorumReconfigMessage::NetworkViewStateRequest;

                let known_nodes = node.network_view.known_nodes();

                let contacted_nodes = known_nodes.len();

                info!("{:?} // Broadcasting view state request to known nodes {:?}", node.network_view.node_id(), known_nodes);

                let message = ReconfigurationMessage::new(seq_no.next_seq(), ReconfigurationMessageType::QuorumReconfig(reconf_message));

                let _ = network_node.broadcast_reconfig_message(message, known_nodes.into_iter());

                timeouts.timeout_reconfig_request(TIMEOUT_DUR, (contacted_nodes / 2 + 1) as u32, seq_no.curr_seq());

                self.current_state = ClientState::Initializing(contacted_nodes, Default::default(), Default::default());

                QuorumNodeResponse::Nil
            }
            _ => {
                /* Timeouts are not relevant when we are in these phases, as we are not actively waiting for the results of our operations*/
                QuorumNodeResponse::Nil
            }
        }
    }
}