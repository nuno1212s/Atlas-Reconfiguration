use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use getset::Getters;

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use atlas_common::crypto::hash::Digest;

use atlas_common::crypto::signature::Signature;
use atlas_common::crypto::threshold_crypto::thold_crypto::dkg::{Ack, DealerPart};
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::peer_addr::PeerAddr;
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::message_signing::NetworkMessageSignatureVerifier;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::serialize::{Buf, Serializable};
use atlas_core::serialize::ReconfigurationProtocolMessage;
use atlas_core::timeouts::RqTimeout;

use crate::QuorumView;
use crate::network_reconfig::KnownNodes;

pub(crate) mod signatures;

/// Used to request to join the current quorum
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumEnterRequest {
    node_triple: NodeTriple,
}

/// Reason message for the rejection of quorum entering request
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumEnterRejectionReason {
    NotAuthorized,
    MissingValues,
    IncorrectNetworkViewSeq,
    NodeIsNotQuorumParticipant,
    FailedToObtainQuorum,
    AlreadyPartOfQuorum,
    CurrentlyReconfiguring,
}

/// A response to a network join request
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumEnterResponse {
    // We have accepted the request to join the quorum
    Successful(QuorumView),

    Rejected(QuorumEnterRejectionReason),
}

#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumLeaveRequest {
    node_triple: NodeTriple,
}

#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumLeaveResponse {
    network_view_seq: SeqNo,

    requesting_node: NodeId,
    origin_node: NodeId,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct KnownNodesMessage {
    nodes: Vec<NodeTriple>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NodeTriple {
    node_id: NodeId,
    node_type: NodeType,
    addr: PeerAddr,
    pub_key: Vec<u8>,
}

/// The response to the request to join the network
/// Returns the list of known nodes in the network, including the newly added node
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum NetworkJoinResponseMessage {
    Successful(Signature, KnownNodesMessage),
    Rejected(NetworkJoinRejectionReason),
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum NetworkJoinRejectionReason {
    NotAuthorized,
    MissingValues,
    IncorrectSignature,
    // Clients don't need to connect to other clients, for example, so it is not necessary
    // For them to know about each other
    NotNecessary,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ReconfigurationMessage {
    seq: SeqNo,
    message_type: ReconfigurationMessageType,
}

/// Reconfiguration message type
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ReconfigurationMessageType {
    // The network reconfiguration protocol messages
    NetworkReconfig(NetworkReconfigMessage),
    QuorumReconfig(QuorumReconfigMessage),
    // Messages related to the threshold crypto protocol
    ThresholdCrypto(ThresholdMessages),
    // The messages related to the configuration of the quorum
    QuorumConfig(ParticipatingQuorumMessage),
}

pub type NetworkJoinCert = (NodeId, Signature);

/// Messages related to the threshold cryptography protocols
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ThresholdMessages {
    // Trigger a new distributed key generation algorithm, with the given
    // Quorum members and threshold
    TriggerDKG(ThresholdDKGArgs),
    // The ordered broadcast messages relating to the ordered broadcast protocol
    DkgDealer(OrderedBCastMessage<DealerPart>),
    DkgAck(OrderedBCastMessage<Ack>),
}

/// The arguments for the distributed key generation protocol
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ThresholdDKGArgs {
    // The quorum that should participate in the key generation protocol
    pub quorum: Vec<NodeId>,
    // The threshold
    pub threshold: usize,
}

/// Network reconfiguration messages (Related only to the network view)
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum NetworkReconfigMessage {
    NetworkJoinRequest(NodeTriple),
    NetworkJoinResponse(NetworkJoinResponseMessage),
    NetworkHelloRequest(NodeTriple, Vec<NetworkJoinCert>),
    NetworkHelloReply(KnownNodesMessage),
}

/// A certificate that a given node sent a quorum view
pub type QuorumViewCert = StoredMessage<QuorumView>;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ReconfigQuorumMessage {
    /// The first message step in the quorum reconfiguration protocol
    JoinMessage(NodeId),

    ConfirmJoin(NodeId),
}

#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumReconfigMessage {
    /// A state request for the current network view
    NetworkViewStateRequest,
    /// The response to the state request
    NetworkViewState(QuorumView),
    /// A request to join the current quorum
    QuorumEnterRequest(QuorumEnterRequest),
    /// The quorum reconfiguration message type, exchanged between quorum members
    /// to reconfigure the quorum
    QuorumReconfig(ReconfigQuorumMessage),
    /// Responses to the requesting node.
    /// These can be delivered either right upon reception (in case the node doesn't qualify
    /// or we are currently reconfiguring)
    QuorumEnterResponse(QuorumEnterResponse),
    /// An update given to the quorum members, to update their quorum view when a new node joins
    /// or leaves the quorum
    QuorumUpdate(QuorumView),
    /// A request to leave the current quorum
    QuorumLeaveRequest(QuorumLeaveRequest),
    /// The response to the request to leave the quorum
    QuorumLeaveResponse(QuorumLeaveResponse),
}

/// Messages that will be sent via channel to the reconfiguration module
pub enum ReconfigMessage {
    TimeoutReceived(Vec<RqTimeout>)
}

/// Messages relating to the ordered broadcast protocol
/// Which is used to order the messages that are sent by the threshold cryptography protocols
///
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum OrderedBCastMessage<T> {
    // Participate on the ordered broadcast protocol with a value
    Value(T),
    //TODO: Make this verifiable such that the leader cannot
    // Invent new values that were not sent to him
    Order(Vec<T>),
    // Vote on a given order of the broadcast
    OrderVote(Digest),
}

/// A type to encapsulate all of the operation messages
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum OperationMessage {
    QuorumInfoOp(QuorumObtainInfoOpMessage),
    QuorumJoinOp(QuorumJoinOpMessage),
}

/// Messages related to the [QuorumObtainInformationOperation]
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumObtainInfoOpMessage {
    // Request information about the current quorum
    RequestInformationMessage,
    // A response message containing the information about the quorum
    QuorumInformationResponse(QuorumView),
}

/// Messages related to the [NodeQuorumJoinOperation]
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumJoinOpMessage {
    // A request made to join the quorum, to be broadcast to the quorum
    RequestJoinQuorum,
    // A response message, to be broadcast to the quorum
    JoinQuorumResponse(QuorumEnterResponse),
}

/// Messages to be exchanged when a node is attempting to join the quorum
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ParticipatingQuorumMessage {
    RequestJoinQuorum,
    // A response message by a quorum member, with the response about the request to enter
    // The quorum. With 2f + 1 of these responses, we can form a certificate
    // Which will be accepted by all correct members of the quorum
    LockedQuorumResponse(QuorumJoinResponse),
    // The node has been accepted into the quorum
    CommitQuorum(LockedQC),
    // The response to the commit quorum request, the last step in the quorum reconfiguration protocol
    CommitQuorumResponse(QuorumCommitResponse),
    // The commit certificate for the quorum.
    // Any replica that receives this, will move to the new quorum
    // And notify it's ordering protocol that it has done so.
    Decided(CommittedQC),
}

/// The locked quorum certificate, containing all of the accepts sent by 2f+1 of the replicas
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct LockedQC {
    proofs: Vec<StoredMessage<QuorumAcceptResponse>>,
}

/// The committed quorum certificate, containing all of the commits sent by 2f+1 of the replicas
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct CommittedQC {
    proofs: Vec<StoredMessage<QuorumCommitAcceptResponse>>,
}

/// The message that will be sent to the reconfiguration module
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumJoinResponse {
    // We have been accepted into the quorum
    Accepted(QuorumAcceptResponse),
    // We have been rejected to join the quorum
    Rejected(QuorumRejectionReason),
}

/// The response to the quorum commit request
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumCommitResponse {
    Accepted(QuorumCommitAcceptResponse),
    Rejected(QuorumRejectionReason),
}

/// The accept response for the quorum commit request, containing the next quorum view
#[derive(Clone, Getters)]
#[get = "pub"]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumCommitAcceptResponse {
    view: QuorumView,
    // Include the locked QC we are voting on so that we
    // Are effectively signing the already established certificate
    // Adding a big layer of security
    // It also guarantees that a replica can't commit vote for any round if that
    // Round is not yet locked.
    qc: LockedQC
}

/// The accept response for the quorum join request, containing the next quorum view
#[derive(Clone, Getters)]
#[get = "pub"]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumAcceptResponse {
    // The view of the quorum
    view: QuorumView,
}

/// The rejection reason for the quorum join request
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumRejectionReason {
    NotAuthorized,
    AlreadyAccepting,
    AlreadyAPartOfQuorum,
    SeqNoTooOld,
    SeqNoTooAdvanced
}

impl ReconfigurationMessage {
    pub fn new(seq: SeqNo, message_type: ReconfigurationMessageType) -> Self {
        Self { seq, message_type }
    }

    pub fn into_inner(self) -> (SeqNo, ReconfigurationMessageType) {
        (self.seq, self.message_type)
    }
}

impl NodeTriple {
    pub fn new(node_id: NodeId, public_key: Vec<u8>, address: PeerAddr, node_type: NodeType) -> Self {
        Self {
            node_id,
            node_type,
            addr: address,
            pub_key: public_key,
        }
    }

    pub fn node_type(&self) -> NodeType {
        self.node_type
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn public_key(&self) -> &Vec<u8> {
        &self.pub_key
    }

    pub fn addr(&self) -> &PeerAddr {
        &self.addr
    }
}

impl From<&KnownNodes> for KnownNodesMessage {
    fn from(value: &KnownNodes) -> Self {
        let mut known_nodes = Vec::with_capacity(value.node_info().len());

        value.node_info().iter().for_each(|(node_id, node_info)| {
            known_nodes.push(NodeTriple {
                node_id: *node_id,
                node_type: node_info.node_type(),
                addr: node_info.addr().clone(),
                pub_key: node_info.pk().pk_bytes().to_vec(),
            })
        });

        KnownNodesMessage {
            nodes: known_nodes,
        }
    }
}

impl KnownNodesMessage {
    pub fn known_nodes(&self) -> &Vec<NodeTriple> {
        &self.nodes
    }

    pub fn into_nodes(self) -> Vec<NodeTriple> {
        self.nodes
    }
}

impl Debug for NodeTriple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeTriple {{ node_id: {:?}, addr: {:?}, type: {:?}}}", self.node_id, self.addr, self.node_type)
    }
}

pub struct ReconfData;

impl Serializable for ReconfData {
    type Message = ReconfigurationMessage;

    fn verify_message_internal<NI, SV>(info_provider: &Arc<NI>, header: &Header, msg: &Self::Message) -> atlas_common::error::Result<()>
        where NI: NetworkInformationProvider, SV: NetworkMessageSignatureVerifier<Self, NI>, Self: Sized {
        Ok(())
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::messages_capnp::system::Builder, msg: &Self::Message) -> atlas_common::error::Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::messages_capnp::system::Reader) -> atlas_common::error::Result<Self::Message> {
        todo!()
    }
}

impl Orderable for ReconfigurationMessage {
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl ReconfigurationMessage {
    pub fn seq(&self) -> SeqNo {
        self.seq
    }
    pub fn message_type(&self) -> &ReconfigurationMessageType {
        &self.message_type
    }
}

impl ReconfigurationProtocolMessage for ReconfData {
    type QuorumJoinCertificate = ();
}

impl QuorumEnterRequest {
    pub fn new(node_triple: NodeTriple) -> Self {
        Self { node_triple }
    }

    pub fn into_inner(self) -> NodeTriple {
        self.node_triple
    }
}

impl ThresholdDKGArgs {
    pub fn init_args(quorum: Vec<NodeId>, threshold: usize) -> Self {
        Self {
            quorum,
            threshold,
        }
    }
}

impl Debug for QuorumReconfigMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {

        // Debug implementation for QuorumReconfigMessage
        match self {
            QuorumReconfigMessage::NetworkViewStateRequest => write!(f, "NetworkViewStateRequest"),
            QuorumReconfigMessage::NetworkViewState(quorum_view) => write!(f, "NetworkViewState()"),
            QuorumReconfigMessage::QuorumEnterRequest(quorum_enter_request) => write!(f, "QuorumEnterRequest()"),
            QuorumReconfigMessage::QuorumEnterResponse(quorum_enter_response) => write!(f, "QuorumEnterResponse()"),
            QuorumReconfigMessage::QuorumLeaveRequest(quorum_leave_request) => write!(f, "QuorumLeaveRequest()"),
            QuorumReconfigMessage::QuorumLeaveResponse(quorum_leave_response) => write!(f, "QuorumLeaveResponse()"),
            QuorumReconfigMessage::QuorumReconfig(reconf) => {
                write!(f, "QuorumReconfig({:?})", reconf)
            }
            QuorumReconfigMessage::QuorumUpdate(view) => {
                write!(f, "QuorumUpdate({:?})", view)
            }
        }
    }
}

impl QuorumAcceptResponse {
    pub fn init(view: QuorumView) -> Self {
        Self {
            view,
        }
    }
}

impl QuorumCommitAcceptResponse {
    pub fn init(view: QuorumView, locked_qc: LockedQC) -> Self {
        Self {
            view,
            qc: locked_qc,
        }
    }
}

impl LockedQC {
    pub fn new(proofs: Vec<StoredMessage<QuorumAcceptResponse>>) -> Self {
        Self {
            proofs,
        }
    }

    pub fn quorum(&self) -> &QuorumView {
        &self.proofs.first().unwrap().message().view()
    }

    pub fn proofs(&self) -> &Vec<StoredMessage<QuorumAcceptResponse>> {
        &self.proofs
    }

    pub fn into_inner(self) -> Vec<StoredMessage<QuorumAcceptResponse>> {
        self.proofs
    }
}

impl CommittedQC {
    pub fn new(proofs: Vec<StoredMessage<QuorumCommitAcceptResponse>>) -> Self {
        Self {
            proofs,
        }
    }

    pub fn proofs(&self) -> &Vec<StoredMessage<QuorumCommitAcceptResponse>> {
        &self.proofs
    }

    pub fn into_inner(self) -> Vec<StoredMessage<QuorumCommitAcceptResponse>> {
        self.proofs
    }
}

