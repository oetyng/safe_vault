// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    interaction::push_state,
    messaging::{send, send_error, send_support, send_to_nodes},
    role::{AdultRole, Role},
};
use crate::{
    chunks::Chunks,
    node_ops::{MsgType, NodeDuties, NodeDuty, OutgoingMsg},
    Error, Node, Result,
};
use log::{debug, info};
use sn_messaging::{
    node::{NodeMsg, NodeQuery},
    Aggregation, DstLocation, MessageId,
};
use sn_routing::ELDER_SIZE;
use xor_name::XorName;

impl Node {
    pub(crate) async fn handle(&mut self, duty: NodeDuty) -> Result<NodeDuties> {
        if !matches!(duty, NodeDuty::NoOp) {
            debug!("Handling NodeDuty: {:?}", duty);
        }

        match duty {
            NodeDuty::Genesis => {
                self.level_up().await?;
                let elder = self.role.as_elder_mut()?;
                elder.received_initial_sync = true;
                Ok(vec![])
            }
            NodeDuty::EldersChanged {
                our_key,
                our_prefix,
                new_elders,
                newbie,
            } => {
                if newbie {
                    info!("Promoted to Elder on Churn");
                    self.level_up().await?;
                    if self.network_api.our_prefix().await.is_empty()
                        && self.network_api.section_chain().await.len() <= ELDER_SIZE
                    {
                        let elder = self.role.as_elder_mut()?;
                        elder.received_initial_sync = true;
                    }
                    Ok(vec![])
                } else {
                    //info!("Updating our replicas on Churn");
                    //self.update_replicas().await?;
                    let elder = self.role.as_elder_mut()?;
                    let msg_id =
                        MessageId::combine(&[our_prefix.name().0, XorName::from(our_key).0]);
                    let ops = vec![push_state(elder, our_prefix, msg_id, new_elders).await?];
                    elder
                        .meta_data
                        .retain_members_only(self.network_api.our_adults().await)
                        .await?;
                    Ok(ops)
                }
            }
            NodeDuty::AdultsChanged {
                added,
                removed,
                remaining,
            } => {
                let our_name = self.our_name().await;
                Ok(self
                    .role
                    .as_adult_mut()?
                    .reorganize_chunks(our_name, added, removed, remaining)
                    .await)
            }
            NodeDuty::SectionSplit {
                our_key,
                our_prefix,
                our_new_elders,
                their_new_elders,
                sibling_key,
                newbie,
            } => {
                if newbie {
                    info!("Beginning split as Newbie");
                    self.begin_split_as_newbie(our_key, our_prefix).await?;
                    Ok(vec![])
                } else {
                    info!("Beginning split as Oldie");
                    self.begin_split_as_oldie(
                        our_prefix,
                        our_key,
                        sibling_key,
                        our_new_elders,
                        their_new_elders,
                    )
                    .await
                }
            }
            NodeDuty::ProposeOffline(unresponsive_adults) => {
                for adult in unresponsive_adults {
                    self.network_api.propose_offline(adult).await?;
                }
                Ok(vec![])
            }
            // a remote section asks for the replicas of their wallet
            NodeDuty::GetSectionElders { msg_id, origin } => {
                Ok(vec![self.get_section_elders(msg_id, origin).await?])
            }
            //
            // ------- reward reg -------
            NodeDuty::SetNodeWallet { wallet_id, node_id } => {
                let elder = self.role.as_elder_mut()?;
                let members = self.network_api.our_members().await;
                if let Some(age) = members.get(&node_id) {
                    elder.payments.set_node_wallet(node_id, wallet_id, *age);
                    Ok(vec![])
                } else {
                    Err(Error::Logic(format!(
                        "{:?}: Couldn't find node id {} when adding wallet {}",
                        self.network_api.our_prefix().await,
                        node_id,
                        wallet_id
                    )))
                }
            }
            NodeDuty::GetNodeWalletKey { node_name, .. } => {
                let elder = self.role.as_elder_mut()?;
                let members = self.network_api.our_members().await;
                if members.get(&node_name).is_some() {
                    let _wallet = elder.payments.get_node_wallet(&node_name);
                    Ok(vec![]) // not yet implemented
                } else {
                    Err(Error::Logic(format!(
                        "{:?}: Couldn't find node {} when getting wallet.",
                        self.network_api.our_prefix().await,
                        node_name
                    )))
                }
            }
            NodeDuty::ProcessLostMember { name, .. } => {
                info!("Member Lost: {:?}", name);
                let elder = self.role.as_elder_mut()?;
                elder.payments.remove_node_wallet(name);
                elder
                    .meta_data
                    .retain_members_only(self.network_api.our_adults().await)
                    .await?;
                Ok(vec![NodeDuty::SetNodeJoinsAllowed(true)])
            }
            //
            // ---------- Levelling --------------
            NodeDuty::SynchState {
                node_wallets,
                metadata,
            } => Ok(vec![self.synch_state(node_wallets, metadata).await?]),
            NodeDuty::LevelDown => {
                info!("Getting Demoted");
                let capacity = self.used_space.max_capacity().await;
                self.role = Role::Adult(AdultRole {
                    chunks: Chunks::new(self.node_info.root_dir.as_path(), capacity).await?,
                });
                Ok(vec![])
            }
            //
            // -------- Data payment --------
            NodeDuty::GetStoreCost {
                bytes,
                mutable,
                data_name,
                msg_id,
                origin,
            } => {
                let elder = self.role.as_elder_mut()?;
                Ok(vec![
                    elder
                        .payments
                        .get_store_cost(bytes, mutable, data_name, msg_id, origin)
                        .await,
                ])
            }
            NodeDuty::ProcessDataPayment { msg, origin } => {
                let elder = self.role.as_elder_mut()?;
                Ok(vec![elder.payments.process_payment(msg, origin).await?])
            }
            //
            // -------- Immutable chunks --------
            NodeDuty::ReadChunk { read, msg_id } => {
                let adult = self.role.as_adult_mut()?;
                let mut ops = vec![adult.chunks.read(&read, msg_id)];
                ops.extend(adult.chunks.check_storage().await?);
                Ok(ops)
            }
            NodeDuty::WriteChunk {
                write,
                msg_id,
                client_signed,
            } => {
                let adult = self.role.as_adult_mut()?;
                let mut ops = vec![
                    adult
                        .chunks
                        .write(&write, msg_id, client_signed.public_key)
                        .await?,
                ];
                ops.extend(adult.chunks.check_storage().await?);
                Ok(ops)
            }
            NodeDuty::ProcessRepublish { chunk, msg_id, .. } => {
                info!("Processing republish with MessageId: {:?}", msg_id);
                let elder = self.role.as_elder_mut()?;
                Ok(vec![elder.meta_data.republish_chunk(chunk).await?])
            }
            NodeDuty::ReachingMaxCapacity => Ok(vec![self.notify_section_of_our_storage().await?]),
            //
            // ------- Misc ------------
            NodeDuty::IncrementFullNodeCount { node_id } => {
                let elder = self.role.as_elder_mut()?;
                let propose_offline = NodeDuty::ProposeOffline(vec![node_id.into()]);
                elder.meta_data.increase_full_node_count(node_id).await;
                // Accept a new node in place of the full node.
                Ok(vec![NodeDuty::SetNodeJoinsAllowed(true), propose_offline])
            }
            NodeDuty::Send(msg) => {
                send(msg, &self.network_api).await?;
                Ok(vec![])
            }
            NodeDuty::SendError(msg) => {
                send_error(msg, &self.network_api).await?;
                Ok(vec![])
            }
            NodeDuty::SendSupport(msg) => {
                send_support(msg, &self.network_api).await?;
                Ok(vec![])
            }
            NodeDuty::SendToNodes {
                msg,
                targets,
                aggregation,
            } => {
                send_to_nodes(&msg, targets, aggregation, &self.network_api).await?;
                Ok(vec![])
            }
            NodeDuty::SetNodeJoinsAllowed(joins_allowed) => {
                self.network_api.set_joins_allowed(joins_allowed).await?;
                Ok(vec![])
            }
            //
            // ------- Data ------------
            NodeDuty::ProcessRead {
                query,
                msg_id,
                client_signed,
                origin,
            } => {
                // TODO: remove this conditional branching
                // routing should take care of this
                let data_section_addr = query.dst_address();
                if self
                    .network_api
                    .our_prefix()
                    .await
                    .matches(&data_section_addr)
                {
                    let elder = self.role.as_elder_mut()?;
                    Ok(vec![
                        elder
                            .meta_data
                            .read(query, msg_id, client_signed.public_key, origin)
                            .await?,
                    ])
                } else {
                    Ok(vec![NodeDuty::Send(OutgoingMsg {
                        msg: MsgType::Node(NodeMsg::NodeQuery {
                            query: NodeQuery::Metadata {
                                query,
                                client_signed,
                                origin,
                            },
                            id: msg_id,
                        }),
                        dst: DstLocation::Section(data_section_addr),
                        section_source: false,
                        aggregation: Aggregation::None,
                    })])
                }
            }
            NodeDuty::ProcessWrite {
                cmd,
                msg_id,
                origin,
                client_signed,
            } => {
                let elder = self.role.as_elder_mut()?;
                Ok(vec![
                    elder
                        .meta_data
                        .write(cmd, msg_id, client_signed, origin)
                        .await?,
                ])
            }
            // --- Completion of Adult operations ---
            NodeDuty::RecordAdultReadLiveness {
                response,
                correlation_id,
                src,
            } => {
                let elder = self.role.as_elder_mut()?;
                Ok(elder
                    .meta_data
                    .record_adult_read_liveness(correlation_id, response, src)
                    .await?)
            }
            NodeDuty::ReplicateChunk { data, .. } => {
                let adult = self.role.as_adult_mut()?;
                Ok(vec![adult.chunks.store_for_replication(data).await?])
            }
            NodeDuty::NoOp => Ok(vec![]),
        }
    }
}
