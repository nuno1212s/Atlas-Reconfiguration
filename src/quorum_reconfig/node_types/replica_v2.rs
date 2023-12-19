use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use futures::SinkExt;
use log::{debug, warn};
use atlas_common::crypto::hash::Digest;
use atlas_common::crypto::threshold_crypto::thold_crypto::dkg::{Ack, DealerPart, DistributedKeyGenerator};
use atlas_common::node_id::NodeId;
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::ReconfigurationNode;
use crate::message::{OrderedBCastMessage, QuorumViewCert, ReconfData, ReconfigurationMessage};

/// The ordered dealer parts, decided by the leader
struct OrderedDealerParts(Vec<usize, DealerPart>);

/// The ordered Ack messages, decided by the leader and
/// voted for by the other nodes in the quorum
struct OrderedAcks(Vec<(usize, Ack)>);

/// Ordering of messages received our of order
struct PendingOrderedBCastMessages<T> {
    // Pending order messages, to be processed when ready
    pending_order: VecDeque<StoredMessage<OrderedBCastMessage<T>>>,

    // Pending order vote messages, to be processed when ready
    pending_order_vote: VecDeque<StoredMessage<OrderedBCastMessage<T>>>,
}

pub type AckOrderedBCastMessage = OrderedBCastMessage<Ack>;
pub type DealerPartOrderedBCastMessage = OrderedBCastMessage<DealerPart>;

/// In an ordered fashion, we broadcast a message to all
///
struct OrderedBroadcast<T> {
    // Our ID
    our_id: NodeId,

    // The leader of this ordered broadcast
    leader: NodeId,

    // The threshold amount of nodes needed to assure security
    threshold: usize,

    // The members that are partaking in this broadcast
    members: Vec<NodeId>,
    // The pending messages that we still have not processed
    pending_message: PendingOrderedBCastMessages<T>,
    // The current phase of this broadcast
    phase: OrderedBCastPhase<T>,
}

/// The inner synchronous broadcast phase phases
enum OrderedBCastPhase<T> {
    // We are currently collecting the various possible messages
    // Which will need to be ordered and broadcast in a given order
    // By the leader
    CollectionPhase(Vec<T>, BTreeSet<NodeId>),
    // We are currently awaiting the order from the leader
    // In order to begin voting on it
    // We store all the values that we have collected
    AwaitingOrder,
    // Once the leader has decided on the order, we are in the voting phase
    // And we wait for the other nodes to vote on the order
    VotingPhase(Vec<T>, BTreeSet<NodeId>),
    // Once we have received enough votes, the order is decided
    // And the broadcast is complete
    Done(Vec<T>),
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
    // The map of each participating node, along with the index of the node in
    // This join protocol
    participating_nodes: BTreeMap<NodeId, usize>,

    // The distributed key generator
    dkg: DistributedKeyGenerator,

    // The current enum state of the reconfiguration threshold key generation
    // Join protocol
    current_state: JoiningThresholdReplicaState,
}

impl<T> OrderedBroadcast<T> {
    fn is_ready(&self) -> bool {
        if let OrderedBCastPhase::Done(_) = &self.phase {
            true
        } else {
            false
        }
    }

    fn handle_message<NT>(&mut self, node: Arc<NT>, header: Header, bcast_message: OrderedBCastMessage<T>)
        where NT: ReconfigurationNode<ReconfData> + 'static {
        match &mut self.phase {
            OrderedBCastPhase::CollectionPhase(received, voted) => {
                match bcast_message {
                    OrderedBCastMessage::Value(value) => {
                        if voted.insert(header.from()) {
                            received.push(value);

                            if voted.len() >= self.threshold {
                                // We have received enough messages to begin the ordering phase
                                self.phase = OrderedBCastPhase::AwaitingOrder;

                                if self.our_id == self.leader {
                                    // We are the leader, so we can decide on the order
                                    // And broadcast it to the other nodes
                                    self.decide_and_bcast_order(node);
                                }
                            }
                        } else {
                            warn!("Received duplicate message from node {:?}", header.from());
                        }
                    }
                    _ => {
                        self.pending_message.queue_message(header, bcast_message);
                    }
                }
            }
            OrderedBCastPhase::AwaitingOrder if header.from() == self.leader => {
                match bcast_message {
                    OrderedBCastMessage::Value(_) => {}
                    OrderedBCastMessage::Order(order) => {
                        self.phase = OrderedBCastPhase::VotingPhase(order, BTreeSet::new());

                        self.vote_on_order(node);
                    }
                    OrderedBCastMessage::OrderVote(_) => {
                        self.pending_message.queue_message(header, bcast_message);
                    }
                }
            }
            OrderedBCastPhase::AwaitingOrder => {
                warn!("Received message from non-leader node {:?} while awaiting order", header.from());
            }
            OrderedBCastPhase::VotingPhase(order, received_votes) => {
                if received_votes.insert(header.from()) {
                    self.phase = OrderedBCastPhase::Done(std::mem::replace(order, Vec::new()));
                } else {
                    warn!("Received duplicate vote from node {:?}", header.from());
                }
            }
            OrderedBCastPhase::Done(order) => {
                debug!("Received message from node {:?} after broadcast was done", header.from());
            }
        }
    }

    fn decide_and_bcast_order<NT>(&mut self, node: Arc<NT>)
        where NT: ReconfigurationNode<ReconfData> + 'static {
        //node.broadcast_reconfig_message();
    }

    fn vote_on_order<NT>(&mut self, node: Arc<NT>)
        where NT: ReconfigurationNode<ReconfData> + 'static {
        //node.broadcast_reconfig_message();
    }

    fn finish(self) -> Vec<T> {
        if let OrderedBCastPhase::Done(order) = self.phase {
            order
        } else {
            unreachable!("How can we finish a broadcast that is not done?")
        }
    }
}

impl<T> PendingOrderedBCastMessages<T> {
    // Query a message for latter processing
    fn queue_message(&mut self, header: Header, bcast_message: OrderedBCastMessage<T>) {
        match bcast_message {
            OrderedBCastMessage::Order(_) => {
                self.pending_order.push_back(StoredMessage::new(header.clone(), bcast_message));
            }
            OrderedBCastMessage::OrderVote(_) => {
                self.pending_order_vote.push_back(StoredMessage::new(header.clone(), bcast_message));
            }
            _ => unreachable!("How can we query a message that is supposed to be processed immediately?")
        }
    }
}