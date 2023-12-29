mod replica;
pub mod network;
mod client;

pub mod operations;

use std::any::Any;
use std::collections::BTreeMap;
use std::iter::Map;
use std::sync::{Arc, Mutex};
use getset::{Getters, MutGetters};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::collections::HashMap;
use atlas_communication::message::Header;
use atlas_core::reconfiguration_protocol::{QuorumReconfigurationMessage, QuorumReconfigurationResponse, QuorumUpdateMessage, ReconfigurableNodeTypes};
use crate::message::QuorumJoinReconfMessages;
use crate::quorum_config::client::ObservingQuorumNode;
use crate::quorum_config::network::QuorumConfigNetworkNode;
use crate::quorum_config::operations::{Operation, OpExecID};
use crate::quorum_config::replica::QuorumParticipator;
use crate::QuorumProtocolResponse;


/// A node enumerator for the possible node types in the quorum
pub enum QuorumNode {
    // A client of the system, not a part of the quorum but still observant of it
    Client(ObservingQuorumNode),
    // A replica, node which participates in the quorum
    Replica(QuorumParticipator),
}


/// This is a simple observer of the quorum, which might then be extended to support
/// Other features, such as quorum reconfiguration or just keeping track of the quorum
/// (in the case of clients)
#[derive(Clone)]
pub struct QuorumObserver {
    quorum_view: Arc<Mutex<QuorumView>>,
}

/// The type of node we are representing
pub enum NodeType {
    ClientNode(ChannelSyncTx<QuorumUpdateMessage>),
    QuorumNode {
        quorum_communication: ChannelSyncTx<QuorumReconfigurationMessage>,
        quorum_responses: ChannelSyncRx<QuorumReconfigurationResponse>
    }
}

#[derive(Getters, MutGetters)]
/// The node structure that handles all information required by the node
pub struct Node {
    #[get = "pub"]
    observer: QuorumObserver,

    #[getset(get_mut = "pub", get = "pub")]
    data: NodeOpData,

    #[get = "pub"]
    node_type: NodeType,
}

/// A global data cache for operations
/// Data here can be accessible across operations of the same type and
/// across operation types (all info is public)
pub struct NodeOpData {
    op_data: HashMap<OpDataKey, Box<dyn Any>>,
}

/// Management of all the ongoing operations
pub struct OnGoingOperations {
    
    op_seq_gen: SeqNo,

    ongoing_operations: BTreeMap<OpExecID, Box<dyn Operation>>,

}

/// The current view of nodes in the network, as in which of them
/// are currently partaking in the consensus
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumView {
    sequence_number: SeqNo,

    quorum_members: Vec<NodeId>,

    f: usize,
}

impl Orderable for QuorumView {
    fn sequence_number(&self) -> SeqNo {
        self.sequence_number
    }
}

impl QuorumView {
    pub fn empty() -> Self {
        QuorumView {
            sequence_number: SeqNo::ZERO,
            quorum_members: Vec::new(),
            f: 0,
        }
    }

    pub fn with_bootstrap_nodes(bootstrap_nodes: Vec<NodeId>) -> Self {
        QuorumView {
            sequence_number: SeqNo::ZERO,
            quorum_members: bootstrap_nodes,
            f: 1,
        }
    }

    pub fn next_with_added_node(&self, node_id: NodeId, f: usize) -> Self {
        QuorumView {
            sequence_number: self.sequence_number.next(),
            quorum_members: {
                let mut members = self.quorum_members.clone();
                members.push(node_id);
                members
            },
            f,
        }
    }

    pub fn sequence_number(&self) -> SeqNo {
        self.sequence_number
    }

    pub fn quorum_members(&self) -> &Vec<NodeId> {
        &self.quorum_members
    }
}

impl QuorumObserver {
    pub fn from_bootstrap(bootstrap_nodes: Vec<NodeId>) -> Self {
        Self
        {
            quorum_view: Arc::new(Mutex::new(QuorumView::with_bootstrap_nodes(bootstrap_nodes))),
        }
    }

    pub fn current_view(&self) -> QuorumView {
        self.quorum_view.lock().unwrap().clone()
    }

    pub fn install_quorum_view(&self, view: QuorumView) {
        let guard = self.quorum_view.lock().unwrap();

        *guard = view;
    }
}

impl QuorumNode {
    pub fn initialize(bootstrap_nodes: Vec<NodeId>, node_type: ReconfigurableNodeTypes) -> (Self, QuorumObserver) {
        let observer = QuorumObserver::from_bootstrap(bootstrap_nodes);

        let node = match node_type {
            ReconfigurableNodeTypes::ClientNode(_) => {
                Self::Client(ObservingQuorumNode::initialize(observer.clone()))
            }
            ReconfigurableNodeTypes::QuorumNode(_, _) => {
                Self::Replica(QuorumParticipator::initialize(observer.clone()))
            }
        };

        (node, observer)
    }

    pub fn is_client(&self) -> bool {
        match self {
            QuorumNode::Client(_) => true,
            _ => false,
        }
    }

    pub fn is_replica(&self) -> bool {
        match self {
            QuorumNode::Replica(_) => true,
            _ => false,
        }
    }

    pub fn iterate<NT>(&mut self, node: Arc<NT>) -> Result<QuorumProtocolResponse> where NT: QuorumConfigNetworkNode + 'static {
        match self {
            QuorumNode::Client(observer) => {
                Ok(QuorumProtocolResponse::Nil)
            }
            QuorumNode::Replica(replica) => {
                replica.iterate(node)?
            }
        }
    }

    pub fn handle_message<NT>(&mut self, node: &Arc<NT>, header: Header, seq_no: SeqNo, message: QuorumJoinReconfMessages) -> Result<QuorumProtocolResponse>
        where NT: QuorumConfigNetworkNode + 'static {
        return match self {
            QuorumNode::Client(client) => client.handle_message(node, header, message),
            QuorumNode::Replica(replica) => replica.handle_message(node, header, seq_no, message),
        };
    }
}

impl Node {



}

/// The key type for the operation data.
/// we use this because we want to reduce the amount of hash functions we
/// have to run in order to reach our data, so we bundle these two together
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
struct OpDataKey {
    op: &'static str,
    key: &'static str,
}

impl NodeOpData {
    pub fn new() -> Self {
        Self {
            op_data: Default::default()
        }
    }

    pub fn get<T: 'static>(&self, op: &'static str, key: &'static str) -> Option<&T> {
        self.op_data.get((op, key).into()).and_then(|v| v.downcast_ref::<T>())
    }

    pub fn get_mut<T: 'static>(&mut self, op: &'static str, key: &'static str) -> Option<&mut T> {
        self.op_data.get_mut((op, key).into()).and_then(|v| v.downcast_mut::<T>())
    }

    pub fn insert<T: 'static>(&mut self, op: &'static str, key: &'static str, value: T) {
        self.op_data.insert((op, key).into(), Box::new(value));
    }

    pub fn remove<T: 'static>(&mut self, op: &'static str, key: &'static str) -> Option<T> {
        self.op_data.remove((op, key).into()).and_then(|v| v.downcast::<T>().ok())
    }
}

impl From<(&'static str, &'static str)> for OpDataKey {
    fn from(value: (&'static str, &'static str)) -> Self {
        Self {
            op: value.0,
            key: value.1,
        }
    }
}