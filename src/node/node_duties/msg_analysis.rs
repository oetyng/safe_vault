// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::node_ops::{
        AdultDuty, ChunkReplicationCmd, ChunkReplicationDuty, ChunkReplicationQuery, ElderDuty,
        MetadataDuty, NodeDuty, NodeMessagingDuty, NodeOperation, OutgoingMsg, RewardCmd,
        RewardDuty, RewardQuery, TransferCmd, TransferDuty, TransferQuery,
    },
    AdultState, Error, NodeState, Result,
};
use log::{debug, info};
use sn_messaging::{
    client::{Cmd, Query},
    node::{
        NodeCmd, NodeDataQueryResponse, NodeEvent, NodeQuery, NodeQueryResponse, NodeRewardQuery,
        NodeRewardQueryResponse, NodeSystemCmd, NodeSystemQuery, NodeTransferCmd,
        NodeTransferQuery, NodeTransferQueryResponse,
    },
    ClientMessage, DstLocation, EndUser, MessageId, NodeMessage, SrcLocation,
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

    pub fn evaluate_node_msg(&self, msg: NodeMessage, src: SrcLocation) -> Result<NodeOperation> {
        debug!(".. node msg ..");

        use SrcLocation::*;
        match &src {
            Node(_name) => self.match_node_msg(msg, src),
            Section(_name) => self.match_section_msg(msg, src),
            EndUser(_) => Err(Error::InvalidMessage(
                msg.id(),
                "User is not a valid src for NodeMessage".to_string(),
            )),
        }
    }

    pub fn evaluate_accumulated_response_to_client(
        &self,
        msg: ClientMessage,
        dst: DstLocation,
    ) -> Result<NodeOperation> {
        debug!(".. evaluating response to client ..");

        if !matches!(dst, DstLocation::EndUser(_)) {
            return Err(Error::InvalidMessage(
                msg.id(),
                format!(
                    "Accumulated client message needs to have enduser dst! msg: {:?}, dst: {:?}",
                    msg, dst,
                ),
            ));
        }

        let is_query_response = matches!(msg, ClientMessage::QueryResponse { .. });
        let is_cmd_error = matches!(msg, ClientMessage::CmdError { .. });
        if !(is_cmd_error || is_query_response) {
            Err(Error::InvalidMessage(
                msg.id(),
                format!(
                    "Accumulated client msg can only be a cmd error or query response: {:?}.",
                    msg,
                ),
            ))
        } else {
            Ok(NodeMessagingDuty::Send(OutgoingMsg {
                msg: msg.into(),
                dst,
                to_be_aggregated: false,
            })
            .into())
        }
    }

    pub fn evaluate_msg_from_client(
        &self,
        msg: ClientMessage,
        origin: EndUser,
    ) -> Result<NodeOperation> {
        match msg {
            ClientMessage::Query {
                query: Query::Data(query),
                id,
                ..
            } => Ok(MetadataDuty::ProcessRead { query, id, origin }.into()), // TODO: Fix these for type safety
            ClientMessage::Cmd {
                cmd: Cmd::Data { .. },
                id,
                ..
            } => Ok(TransferDuty::ProcessCmd {
                cmd: TransferCmd::ProcessPayment(msg.clone()),
                msg_id: id,
                origin: SrcLocation::EndUser(origin),
            }
            .into()),
            ClientMessage::Cmd {
                cmd: Cmd::Transfer(cmd),
                id,
                ..
            } => Ok(TransferDuty::ProcessCmd {
                cmd: cmd.into(),
                msg_id: id,
                origin: SrcLocation::EndUser(origin),
            }
            .into()),
            ClientMessage::Query {
                query: Query::Transfer(query),
                id,
                ..
            } => Ok(TransferDuty::ProcessQuery {
                query: query.into(),
                msg_id: id,
                origin: SrcLocation::EndUser(origin),
            }
            .into()),
            _ => Err(Error::Logic(format!(
                "Could not evaluate Client Msg {:?}",
                msg
            ))),
        }
    }

    /// Accumulated messages (i.e. src == section)
    fn match_section_msg(&self, msg: NodeMessage, origin: SrcLocation) -> Result<NodeOperation> {
        debug!("Evaluating received msg for Section: {:?}", msg);

        let res = match &msg {
            // ------ metadata ------
            NodeMessage::NodeQuery {
                query: NodeQuery::Data { query, origin },
                id,
                ..
            } => MetadataDuty::ProcessRead {
                query: query.clone(),
                id: *id,
                origin: *origin,
            }
            .into(),
            NodeMessage::NodeCmd {
                cmd: NodeCmd::Data { cmd, origin },
                id,
                ..
            } => MetadataDuty::ProcessWrite {
                cmd: cmd.clone(),
                id: *id,
                origin: *origin,
            }
            .into(),
            //
            // ------ adult ------
            // NodeMessage::NodeQuery {
            //     query:
            //         NodeQuery::Data {
            //             query: DataQuery::Blob(read),
            //             origin,
            //         },
            //     id,
            //     ..
            // } => AdultDuty::RunAsChunkStore(ChunkStoreDuty::ReadChunk {
            //     read: read.clone(),
            //     id: *id,
            //     origin: SrcLocation::EndUser(*origin),
            // })
            // .into(),
            // NodeMessage::NodeCmd {
            //     cmd:
            //         NodeCmd::Data {
            //             cmd: DataCmd::Blob(cmd),
            //             origin,
            //         },
            //     id,
            //     ..
            // } => AdultDuty::RunAsChunkStore(ChunkStoreDuty::WriteChunk {
            //     write: cmd.clone(),
            //     id: *id,
            //     origin: SrcLocation::EndUser(*origin),
            // })
            // .into(),
            //
            // ------ chunk replication ------
            NodeMessage::NodeQuery {
                query:
                    NodeQuery::System(NodeSystemQuery::GetChunk {
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
                let _message = NodeMessage::NodeCmd {
                    cmd: NodeCmd::System(NodeSystemCmd::ReplicateChunk {
                        new_holder: *new_holder,
                        address: *address,
                        current_holders: current_holders.clone(),
                    }),
                    id: msg_id,
                };

                info!("Internal ChunkReplicationQuery ProcessQuery");
                AdultDuty::RunAsChunkReplication(ChunkReplicationDuty::ProcessQuery {
                    query: ChunkReplicationQuery::GetChunk(*address),
                    msg_id,
                    origin,
                })
                .into()
            }
            // this cmd is accumulated, thus has authority
            NodeMessage::NodeCmd {
                cmd:
                    NodeCmd::System(NodeSystemCmd::ReplicateChunk {
                        address,
                        current_holders,
                        ..
                    }),
                id,
                ..
            } => AdultDuty::RunAsChunkReplication(ChunkReplicationDuty::ProcessCmd {
                cmd: ChunkReplicationCmd::ReplicateChunk {
                    current_holders: current_holders.clone(),
                    address: *address,
                },
                msg_id: *id,
                origin,
            })
            .into(),
            //
            // ------ Rewards ------
            NodeMessage::NodeQuery {
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
                origin,
            }
            .into(),
            // trivial to accumulate
            NodeMessage::NodeQueryResponse {
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
                origin,
            }
            .into(),
            // tricky to accumulate, since it has a vec of events.. but we try anyway for now..
            NodeMessage::NodeQueryResponse {
                response:
                    NodeQueryResponse::Transfers(NodeTransferQueryResponse::GetNewSectionWallet(result)),
                id,
                ..
            } => RewardDuty::ProcessCmd {
                cmd: RewardCmd::InitiateSectionWallet(result.clone()?),
                msg_id: *id,
                origin,
            }
            .into(),
            // tricky to accumulate, since it has a vec of events.. but we try anyway for now..
            NodeMessage::NodeQueryResponse {
                response:
                    NodeQueryResponse::Rewards(NodeRewardQueryResponse::GetSectionWalletHistory(result)),
                id,
                ..
            } => RewardDuty::ProcessCmd {
                cmd: RewardCmd::InitiateSectionWallet(result.clone()),
                msg_id: *id,
                origin,
            }
            .into(),
            //
            // ------ transfers --------
            // doesn't need to be accumulated, but makes it a bit slimmer..
            NodeMessage::NodeCmd {
                cmd: NodeCmd::Transfers(NodeTransferCmd::PropagateTransfer(proof)),
                id,
            } => TransferDuty::ProcessCmd {
                cmd: TransferCmd::PropagateTransfer(proof.credit_proof()),
                msg_id: *id,
                origin,
            }
            .into(),
            // tricky to accumulate, since it has a vec of events.. but we try anyway for now..
            NodeMessage::NodeQueryResponse {
                response:
                    NodeQueryResponse::Transfers(NodeTransferQueryResponse::GetReplicaEvents(events)),
                id,
                ..
            } => TransferDuty::ProcessCmd {
                cmd: TransferCmd::InitiateReplica(events.clone()?),
                msg_id: *id,
                origin,
            }
            .into(),
            // doesn't need to be accumulated, but makes it a bit slimmer..
            NodeMessage::NodeCmd {
                cmd: NodeCmd::Transfers(NodeTransferCmd::RegisterSectionPayout(debit_agreement)),
                id,
            } => TransferDuty::ProcessCmd {
                cmd: TransferCmd::RegisterSectionPayout(debit_agreement.clone()),
                msg_id: *id,
                origin,
            }
            .into(),
            // Accumulates at remote section, for security
            NodeMessage::NodeQuery {
                query: NodeQuery::Transfers(NodeTransferQuery::GetNewSectionWallet(public_key)),
                id,
            } => TransferDuty::ProcessQuery {
                query: TransferQuery::GetNewSectionWallet(*public_key),
                msg_id: *id,
                origin,
            }
            .into(),
            // // ... so... we accumulate a query response, hmm
            // NodeMessage::NodeQueryResponse {
            //     response:
            //         NodeQueryResponse::Rewards(
            //             NodeRewardQueryResponse::GetSectionWalletHistory(info),
            //         ),
            //     ..
            // } => {
            //     info!("We have a GetSectionWalletHistory query response!");
            //     NodeDuty::InitSectionWallet(info.clone()).into()
            // }
            NodeMessage::NodeEvent {
                event: NodeEvent::SectionPayoutRegistered { from, to },
                ..
            } => NodeDuty::FinishElderChange {
                previous_key: *from,
                new_key: *to,
            }
            .into(),
            _ => {
                return Err(Error::Logic(format!(
                    "Could not evaluate accumulated msg: {:?}",
                    msg
                )))
            }
        };

        Ok(res)
    }

    fn match_node_msg(&self, msg: NodeMessage, origin: SrcLocation) -> Result<NodeOperation> {
        debug!("Evaluating received msg for Node: {:?}", msg);

        let res = match &msg {
            //
            // ------ wallet register ------
            NodeMessage::NodeCmd {
                cmd: NodeCmd::System(NodeSystemCmd::RegisterWallet { wallet, .. }),
                id,
                ..
            } => RewardDuty::ProcessCmd {
                cmd: RewardCmd::SetNodeWallet {
                    wallet_id: *wallet,
                    node_id: origin.to_dst().name().unwrap(),
                },
                msg_id: *id,
                origin,
            }
            .into(),
            //
            // ------ system cmd ------
            NodeMessage::NodeCmd {
                cmd: NodeCmd::System(NodeSystemCmd::StorageFull { node_id, .. }),
                ..
            } => ElderDuty::StorageFull { node_id: *node_id }.into(),

            //
            // ------ node duties ------
            NodeMessage::NodeCmd {
                cmd: NodeCmd::System(NodeSystemCmd::ProposeGenesis { credit, sig }),
                ..
            } => NodeDuty::ReceiveGenesisProposal {
                credit: credit.clone(),
                sig: sig.clone(),
            }
            .into(),
            NodeMessage::NodeCmd {
                cmd: NodeCmd::System(NodeSystemCmd::AccumulateGenesis { signed_credit, sig }),
                ..
            } => NodeDuty::ReceiveGenesisAccumulation {
                signed_credit: signed_credit.clone(),
                sig: sig.clone(),
            }
            .into(),
            //
            //
            // ------ chunk replication ------
            // query response from adult cannot be accumulated
            NodeMessage::NodeQueryResponse {
                response: NodeQueryResponse::Data(NodeDataQueryResponse::GetChunk(result)),
                correlation_id,
                ..
            } => {
                let blob = result.to_owned()?;
                info!("Verifying GetChunk NodeQueryResponse!");
                // Recreate original MessageId from Section
                let msg_id =
                    MessageId::combine(vec![*blob.address().name(), self.state.node_name()]);
                if msg_id == *correlation_id {
                    AdultDuty::RunAsChunkReplication(ChunkReplicationDuty::ProcessCmd {
                        cmd: ChunkReplicationCmd::StoreReplicatedBlob(blob),
                        msg_id,
                        origin,
                    })
                    .into()
                } else {
                    info!("Given blob is incorrect.");
                    panic!()
                }
            }
            //
            // ------ nonacc rewards ------
            // validated event cannot be accumulated at routing, since it has sig shares
            NodeMessage::NodeEvent {
                event: NodeEvent::SectionPayoutValidated(validation),
                id,
                ..
            } => RewardDuty::ProcessCmd {
                cmd: RewardCmd::ReceivePayoutValidation(validation.clone()),
                msg_id: *id,
                origin,
            }
            .into(),
            //
            // ------ nonacc transfers ------
            // queries are from single source, so cannot be accumulated
            NodeMessage::NodeQuery {
                query: NodeQuery::Transfers(NodeTransferQuery::GetReplicaEvents),
                id,
            } => TransferDuty::ProcessQuery {
                query: TransferQuery::GetReplicaEvents,
                msg_id: *id,
                origin,
            }
            .into(),
            // cannot be accumulated due to having sig share
            NodeMessage::NodeCmd {
                cmd: NodeCmd::Transfers(NodeTransferCmd::ValidateSectionPayout(signed_transfer)),
                id,
            } => TransferDuty::ProcessCmd {
                cmd: TransferCmd::ValidateSectionPayout(signed_transfer.clone()),
                msg_id: *id,
                origin,
            }
            .into(),
            // from a single src, so cannot be accumulated
            NodeMessage::NodeQuery {
                query: NodeQuery::Rewards(NodeRewardQuery::GetSectionWalletHistory),
                id,
            } => RewardDuty::ProcessQuery {
                query: RewardQuery::GetSectionWalletHistory,
                msg_id: *id,
                origin,
            }
            .into(),
            _ => {
                return Err(Error::Logic(format!(
                    "Could not evaluate single src msg: {:?}",
                    msg
                )))
            }
        };
        Ok(res)
    }

    fn adult_state(&self) -> Result<&AdultState> {
        if let NodeState::Adult(state) = &self.state {
            Ok(state)
        } else {
            Err(Error::InvalidOperation(
                "Tried to get adult state when there was none.".to_string(),
            ))
        }
    }
}
