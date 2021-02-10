// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::node_ops::{
        AdultDuty, ChunkReplicationCmd, ChunkReplicationDuty, ChunkReplicationQuery,
        ChunkStoreDuty, ElderDuty, MetadataDuty, NodeDuty, NodeOperation, ReceivedMsg, RewardCmd,
        RewardDuty, RewardQuery, TransferCmd, TransferDuty, TransferQuery,
    },
    AdultState, ElderState, Error, NodeState, Result,
};
use log::{debug, error, info};
use sn_messaging::{
    client::{
        Cmd, DataQuery, Message, MessageId, NodeCmd, NodeDataCmd, NodeDataQuery,
        NodeDataQueryResponse, NodeEvent, NodeQuery, NodeQueryResponse, NodeRewardQuery,
        NodeRewardQueryResponse, NodeSystemCmd, NodeTransferCmd, NodeTransferQuery,
        NodeTransferQueryResponse, Query,
    },
    DstLocation,
};

// NB: This approach is not entirely good, so will need to be improved.

/// Evaluates remote msgs from the network,
/// i.e. not msgs sent directly from a client.
pub struct ReceivedMsgAnalysis {
    state: NodeState,
}

impl ReceivedMsgAnalysis {
    pub fn new(state: NodeState) -> Self {
        Self { state }
    }

    pub async fn evaluate(&self, msg: &ReceivedMsg) -> Result<NodeOperation> {
        use AdultDuty::*;
        use ChunkStoreDuty::*;
        use DstLocation::*;

        debug!("Evaluating received msg..");

        let res = match &msg.dst {
            Direct => unimplemented!(),
            User(user) => {
                debug!("Evaluating received msg for User: {:?}", msg);
                match &msg.msg {
                    //
                    // ------ metadata ------
                    Message::Query {
                        query: Query::Data(_),
                        ..
                    } => MetadataDuty::ProcessRead {
                        msg: msg.msg.clone(),
                        origin: *user,
                    }
                    .into(),
                    Message::Cmd {
                        cmd: Cmd::Data { .. },
                        ..
                    } => MetadataDuty::ProcessWrite {
                        msg: msg.msg.clone(),
                        origin: *user,
                    }
                    .into(),

                    _ => {
                        debug!("No match for received msg for User: {:?}", msg);
                        unimplemented!()
                    }
                }
            }
            Section(_name) => {
                debug!("Evaluating received msg for Section: {:?}", msg);
                match &msg.msg {
                    //
                    // ------ wallet register ------
                    Message::NodeCmd {
                        cmd: NodeCmd::System(NodeSystemCmd::RegisterWallet { wallet, .. }),
                        id,
                        ..
                    } => RewardDuty::ProcessCmd {
                        cmd: RewardCmd::SetNodeWallet {
                            wallet_id: *wallet,
                            node_id: msg.src.to_dst().name().unwrap(),
                        },
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    //
                    // ------ system cmd ------
                    Message::NodeCmd {
                        cmd: NodeCmd::System(NodeSystemCmd::StorageFull { node_id, .. }),
                        ..
                    } => ElderDuty::StorageFull { node_id: *node_id }.into(),
                    //
                    // ------ node duties ------
                    Message::NodeCmd {
                        cmd: NodeCmd::System(NodeSystemCmd::ProposeGenesis { credit, sig }),
                        ..
                    } => NodeDuty::ReceiveGenesisProposal {
                        credit: credit.clone(),
                        sig: sig.clone(),
                    }
                    .into(),
                    Message::NodeCmd {
                        cmd:
                            NodeCmd::System(NodeSystemCmd::AccumulateGenesis { signed_credit, sig }),
                        ..
                    } => NodeDuty::ReceiveGenesisAccumulation {
                        signed_credit: signed_credit.clone(),
                        sig: sig.clone(),
                    }
                    .into(),
                    Message::NodeQueryResponse {
                        response:
                            NodeQueryResponse::Transfers(
                                NodeTransferQueryResponse::CatchUpWithSectionWallet(Ok(info)),
                            ),
                        ..
                    } => {
                        info!("We have a CatchUpWithSectionWallet query response!");
                        NodeDuty::InitSectionWallet(info.clone()).into()
                    }
                    Message::NodeEvent {
                        event: NodeEvent::SectionPayoutRegistered { from, to },
                        ..
                    } => NodeDuty::FinishElderChange {
                        previous_key: *from,
                        new_key: *to,
                    }
                    .into(),

                    _ => {
                        debug!("No match for received msg for Section: {:?}", msg);
                        unimplemented!()
                    }
                }
            }
            Node(_name) => {
                debug!("Evaluating received msg for Node: {:?}", msg);
                match &msg.msg {
                    //
                    // ------ adult ------
                    Message::Query {
                        query: Query::Data(DataQuery::Blob(_)),
                        ..
                    } => RunAsChunkStore(ReadChunk(msg.clone())).into(),
                    Message::NodeCmd {
                        cmd: NodeCmd::Data(NodeDataCmd::Blob(_)),
                        ..
                    } => RunAsChunkStore(WriteChunk(msg.clone())).into(),
                    //
                    // ------ chunk replication ------
                    Message::NodeCmd {
                        cmd:
                            NodeCmd::Data(NodeDataCmd::ReplicateChunk {
                                address,
                                current_holders,
                                ..
                            }),
                        id,
                        ..
                    } => {
                        //info!("Origin of Replicate Chunk: {:?}", msg.origin.clone());
                        RunAsChunkReplication(ChunkReplicationDuty::ProcessCmd {
                            cmd: ChunkReplicationCmd::ReplicateChunk {
                                current_holders: current_holders.clone(),
                                address: *address,
                                //section_authority: msg.most_recent_sender().clone(),
                            },
                            msg_id: *id,
                            origin: msg.src,
                        })
                        .into()
                    }
                    Message::NodeQueryResponse {
                        response: NodeQueryResponse::Data(NodeDataQueryResponse::GetChunk(result)),
                        correlation_id,
                        ..
                    } => {
                        let blob = result.to_owned()?;
                        info!("Verifying GetChunk NodeQueryResponse!");
                        // Recreate original MessageId from Section
                        let msg_id = MessageId::combine(vec![
                            *blob.address().name(),
                            self.state.node_name(),
                        ]);
                        if msg_id == *correlation_id {
                            RunAsChunkReplication(ChunkReplicationDuty::ProcessCmd {
                                cmd: ChunkReplicationCmd::StoreReplicatedBlob(blob),
                                msg_id,
                                origin: msg.src,
                            })
                            .into()
                        } else {
                            info!("Given blob is incorrect.");
                            panic!()
                        }
                    }
                    Message::NodeQuery {
                        query:
                            NodeQuery::Data(NodeDataQuery::GetChunk {
                                //section_authority,
                                new_holder,
                                address,
                                current_holders,
                            }),
                        ..
                    } => {
                        info!("Verifying GetChunk query!");
                        let _proof_chain = self.adult_state()?.section_proof_chain();

                        // Recreate original MessageId from Section
                        let msg_id = MessageId::combine(vec![*address.name(), *new_holder]);

                        // Recreate cmd that was sent by the section.
                        let _message = Message::NodeCmd {
                            cmd: NodeCmd::Data(NodeDataCmd::ReplicateChunk {
                                new_holder: *new_holder,
                                address: *address,
                                current_holders: current_holders.clone(),
                            }),
                            id: msg_id,
                        };

                        info!("Internal ChunkReplicationQuery ProcessQuery");
                        RunAsChunkReplication(ChunkReplicationDuty::ProcessQuery {
                            query: ChunkReplicationQuery::GetChunk(*address),
                            msg_id,
                            origin: msg.src,
                        })
                        .into()
                    }
                    //
                    // ------ nonacc rewards ------
                    Message::NodeQuery {
                        query:
                            NodeQuery::Rewards(NodeRewardQuery::GetNodeWalletId {
                                old_node_id,
                                new_node_id,
                            }),
                        id,
                    } => RewardDuty::ProcessQuery {
                        query: RewardQuery::GetNodeWalletId {
                            old_node_id: *old_node_id,
                            new_node_id: *new_node_id,
                        },
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    Message::NodeEvent {
                        event: NodeEvent::SectionPayoutValidated(validation),
                        id,
                        ..
                    } => RewardDuty::ProcessCmd {
                        cmd: RewardCmd::ReceivePayoutValidation(validation.clone()),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    //
                    // ------ nacc rewards ------
                    Message::NodeQueryResponse {
                        response:
                            NodeQueryResponse::Rewards(NodeRewardQueryResponse::GetNodeWalletId(Ok((
                                wallet_id,
                                new_node_id,
                            )))),
                        id,
                        ..
                    } => RewardDuty::ProcessCmd {
                        cmd: RewardCmd::ActivateNodeRewards {
                            id: *wallet_id,
                            node_id: *new_node_id,
                        },
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    Message::NodeQueryResponse {
                        response:
                            NodeQueryResponse::Transfers(
                                NodeTransferQueryResponse::GetNewSectionWallet(result),
                            ),
                        id,
                        ..
                    } => RewardDuty::ProcessCmd {
                        cmd: RewardCmd::InitiateSectionWallet(result.clone()?),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    Message::NodeQueryResponse {
                        response:
                            NodeQueryResponse::Transfers(
                                NodeTransferQueryResponse::CatchUpWithSectionWallet(result),
                            ),
                        id,
                        ..
                    } => RewardDuty::ProcessCmd {
                        cmd: RewardCmd::InitiateSectionWallet(result.clone()?),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    //
                    // ------ acc transfers ------
                    Message::NodeQueryResponse {
                        response:
                            NodeQueryResponse::Transfers(NodeTransferQueryResponse::GetReplicaEvents(
                                events,
                            )),
                        id,
                        ..
                    } => TransferDuty::ProcessCmd {
                        cmd: TransferCmd::InitiateReplica(events.clone()?),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    //
                    // ------ nonacc transfers ------
                    Message::NodeCmd {
                        cmd: NodeCmd::Transfers(NodeTransferCmd::PropagateTransfer(proof)),
                        id,
                    } => TransferDuty::ProcessCmd {
                        cmd: TransferCmd::PropagateTransfer(proof.credit_proof()),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    Message::NodeQuery {
                        query: NodeQuery::Transfers(NodeTransferQuery::GetReplicaEvents(public_key)),
                        id,
                    } => {
                        // This comparison is a good example of the need to use `lazy messaging`,
                        // as to handle that the expected public key is not the same as the current.
                        if public_key == &self.elder_state()?.section_public_key() {
                            TransferDuty::ProcessQuery {
                                query: TransferQuery::GetReplicaEvents,
                                msg_id: *id,
                                origin: msg.src,
                            }
                            .into()
                        } else {
                            error!("Unexpected public key!");
                            return Err(Error::Logic("Unexpected PK".to_string()));
                        }
                    }
                    Message::NodeCmd {
                        cmd:
                            NodeCmd::Transfers(NodeTransferCmd::ValidateSectionPayout(signed_transfer)),
                        id,
                    } => TransferDuty::ProcessCmd {
                        cmd: TransferCmd::ValidateSectionPayout(signed_transfer.clone()),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    Message::NodeCmd {
                        cmd:
                            NodeCmd::Transfers(NodeTransferCmd::RegisterSectionPayout(debit_agreement)),
                        id,
                    } => TransferDuty::ProcessCmd {
                        cmd: TransferCmd::RegisterSectionPayout(debit_agreement.clone()),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    Message::NodeQuery {
                        query:
                            NodeQuery::Transfers(NodeTransferQuery::CatchUpWithSectionWallet(
                                public_key,
                            )),
                        id,
                    } => TransferDuty::ProcessQuery {
                        query: TransferQuery::CatchUpWithSectionWallet(*public_key),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    Message::NodeQuery {
                        query:
                            NodeQuery::Transfers(NodeTransferQuery::GetNewSectionWallet(public_key)),
                        id,
                    } => TransferDuty::ProcessQuery {
                        query: TransferQuery::GetNewSectionWallet(*public_key),
                        msg_id: *id,
                        origin: msg.src,
                    }
                    .into(),
                    _ => {
                        debug!("No match for received msg for Node: {:?}", msg);
                        unimplemented!()
                    }
                }
            }
        };

        Ok(res)
    }

    fn elder_state(&self) -> Result<&ElderState> {
        if let NodeState::Elder(state) = &self.state {
            Ok(state)
        } else {
            Err(Error::InvalidOperation)
        }
    }

    fn adult_state(&self) -> Result<&AdultState> {
        if let NodeState::Adult(state) = &self.state {
            Ok(state)
        } else {
            Err(Error::InvalidOperation)
        }
    }
}
