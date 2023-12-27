use std::collections::BTreeMap;
use std::sync::Arc;

use atlas_common::crypto::threshold_crypto::thold_crypto::dkg::{Ack, DealerPart, DistributedKeyGenerator, DKGParams};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::ReconfigurationNode;

use crate::message::{OrderedBCastMessage, ReconfData, ReconfigurationMessageType, ThresholdDKGArgs, ThresholdMessages};
use crate::quorum_reconfig::ordered_bcast::OrderedBroadcast;

/// The ordered dealer parts, decided by the leader
struct OrderedDealerParts(Vec<usize, DealerPart>);

/// The ordered Ack messages, decided by the leader and
/// voted for by the other nodes in the quorum
struct OrderedAcks(Vec<(usize, Ack)>);

pub type AckOrderedBCastMessage = OrderedBCastMessage<Ack>;
pub type DealerPartOrderedBCastMessage = OrderedBCastMessage<DealerPart>;

/// Pending messages that we have not yet processed
pub struct PendingMessages {
    // The pending dealer part ordered broadcast messages
    pending_dealer_messages: Vec<StoredMessage<OrderedBCastMessage<DealerPart>>>,
    // The pending ack part ordered broadcast messages
    pending_ack_messages: Vec<StoredMessage<OrderedBCastMessage<Ack>>>,
}

/// The current state of our joining protocol.
/// We want to achieve a common order in a byzantine
/// fault tolerant scenario
enum JoiningThresholdReplicaState {
    // Initial state, where we have done nothing at all
    Init,
    // exchanging dealer parts and attaining a global ordering for them
    PartExchange(OrderedBroadcast<DealerPart>),
    // Exchanging acks and attaining a global ordering for them
    AckExchange(OrderedBroadcast<Ack>),
}

/// A node that is currently attempting to join the network
struct JoiningThresholdReplica {
    // The quorum partaking in this
    quorum: Vec<NodeId>,

    threshold: usize,

    // The map of each participating node, along with the index of the node in
    // This join protocol
    participating_nodes: BTreeMap<NodeId, usize>,

    // The distributed key generator
    dkg: DistributedKeyGenerator,

    // The current enum state of the reconfiguration threshold key generation
    // Join protocol
    current_state: JoiningThresholdReplicaState,
    // Messages that we received while we were in the middle of another
    // Incompatible phase
    pending_messages: PendingMessages,
}

impl JoiningThresholdReplica {
    /// Initialize a distributed key generation protocol
    pub fn initialize_dkg_protocol<NT>(our_id: NodeId, quorum: Vec<NodeId>, threshold: usize, node: Arc<NT>)
                                       -> Result<Self>
        where NT: ReconfigurationNode<ReconfData> + 'static {
        let trigger_dkg = ThresholdMessages::TriggerDKG(ThresholdDKGArgs::init_args(quorum.clone(), threshold));

        //TODO: Send quiet_unwrap!(node.broadcast_reconfig_message(trigger_dkg, quorum.clone().into_iter()));

        let mut participating_nodes = Default::default();

        quorum.iter().enumerate().for_each(|(id, node)| {
            participating_nodes.insert(*node, id + 1);
        });

        let params = DKGParams::new(quorum.len(), threshold);

        let participating_node_id = participating_nodes.get(&our_id);

        let (dkg, dealer) = DistributedKeyGenerator::new(params, participating_node_id)?;

        let ordered_bcast = OrderedBroadcast::<DealerPart>::init_ordered_bcast(our_id, our_id, threshold, quorum.clone(),
                                                                               |bcast| ReconfigurationMessageType::ThresholdCrypto(ThresholdMessages::DkgDealer(bcast)));

        let reconfig_msg_type = ReconfigurationMessageType::ThresholdCrypto(ThresholdMessages::DkgDealer(OrderedBCastMessage::Value(dealer)));

        //TODO: Send message

        Ok(Self {
            quorum,
            threshold,
            participating_nodes,
            dkg,
            current_state: JoiningThresholdReplicaState::Init,
            pending_messages: PendingMessages { pending_dealer_messages: Vec::new(), pending_ack_messages: Vec::new() },
        })
    }

    /// Initialize the protocol from an init message
    pub fn from_init_message<NT>(our_id: NodeId, dkg_args: ThresholdDKGArgs, node: Arc<NT>) -> Result<Self>
        where NT: ReconfigurationNode<ReconfData> + 'static {
        Self::initialize_dkg_protocol(our_id, dkg_args.quorum, dkg_args.threshold, node)
    }

    fn iterate<NT>(&mut self, node: Arc<NT>)
        where NT: ReconfigurationNode<ReconfData> + 'static {
        match &mut self.current_state {
            JoiningThresholdReplicaState::Init => {}
            JoiningThresholdReplicaState::PartExchange(dealer_part_exchange) => {}
            JoiningThresholdReplicaState::AckExchange(ack_part_exchange) => {}
        }
    }

    fn handle_message<NT>(&mut self, node: Arc<NT>, header: Header, message: ThresholdMessages)
        where NT: ReconfigurationNode<ReconfData> + 'static {
        match &mut self.current_state {
            JoiningThresholdReplicaState::Init => {}
            JoiningThresholdReplicaState::PartExchange(exchange) => {
                match message {
                    ThresholdMessages::TriggerDKG(dk) => {}
                    ThresholdMessages::DkgDealer(dealer_part) => {
                        exchange.handle_message(node, header, dealer_part);
                    }
                    ThresholdMessages::DkgAck(ack_parts) => self.pending_messages.queue_message(header, ThresholdMessages::DkgAck(ack_parts)),
                }
            }
            JoiningThresholdReplicaState::AckExchange(ack_part) => {
                match message {
                    ThresholdMessages::TriggerDKG(dk) => {}
                    ThresholdMessages::DkgDealer(dealer_part) => self.pending_messages.queue_message(header, ThresholdMessages::DkgDealer(dealer_part)),
                    ThresholdMessages::DkgAck(ack_parts) => ack_part.handle_message(node, header, ack_parts),
                }
            }
        }
    }
}

impl PendingMessages {
    fn queue_message(&mut self, header: Header, message: ThresholdMessages) {
        match message {
            ThresholdMessages::DkgDealer(dealer_part) => {
                self.pending_dealer_messages.push(StoredMessage::new(header, dealer_part));
            }
            ThresholdMessages::DkgAck(ack) => {
                self.pending_ack_messages.push(StoredMessage::new(header, ack));
            }
            _ => unreachable!("Invalid message type for pending messages")
        }
    }
}