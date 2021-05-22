// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub mod elder_signing;
mod reward_calc;
pub mod reward_wallets;

use self::{reward_calc::distribute_rewards, reward_wallets::RewardWallets};
use crate::{
    capacity::RateLimit,
    node_ops::{MsgType, NodeDuty, OutgoingMsg},
    utils, Error, Result,
};
use log::{info, warn};
use sn_data_types::{NodeAge, PublicKey, Token};
use sn_dbc::{Dbc, Mint, MintRequest};
use sn_messaging::{
    client::{
        ClientMsg, ClientSigned, Cmd, CmdError, DataCmd, PaymentError, ProcessMsg, QueryResponse,
        StoreCost,
    },
    node::{NodeCmd, NodeMsg},
    Aggregation, DstLocation, EndUser, MessageId, SrcLocation,
};
use sn_routing::{Prefix, XorName};
use std::collections::BTreeMap;

/// The management of section funds,
/// via the usage of a distributed AT2 Actor.
#[allow(clippy::large_enum_variant)]
pub struct Payments {
    rate_limit: RateLimit,
    wallets: RewardWallets,
    mint: Mint,
}

impl Payments {
    ///
    pub fn new(rate_limit: RateLimit, wallets: RewardWallets, mint: Mint) -> Self {
        Self {
            rate_limit,
            wallets,
            mint,
        }
    }

    /// Returns registered wallet key of a node.
    pub fn get_node_wallet(&self, node_name: &XorName) -> Option<PublicKey> {
        let (_, key) = self.wallets.get(node_name)?;
        Some(key)
    }

    /// Returns node wallet keys of registered nodes.
    pub fn node_wallets(&self) -> BTreeMap<XorName, (NodeAge, PublicKey)> {
        self.wallets.node_wallets()
    }

    /// Nodes register/updates wallets for future reward payouts.
    pub fn set_node_wallet(&self, node_id: XorName, wallet: PublicKey, age: u8) {
        self.wallets.set_node_wallet(node_id, age, wallet)
    }

    /// When the section becomes aware that a node has left,
    /// its reward key is removed.
    pub fn remove_node_wallet(&self, node_name: XorName) {
        self.wallets.remove_wallet(node_name)
    }

    /// When the section becomes aware that a node has left,
    /// its reward key is removed.
    pub fn keep_wallets_of(&self, prefix: Prefix) {
        self.wallets.keep_wallets_of(prefix)
    }

    /// Get latest StoreCost for the given number of bytes.
    pub async fn get_store_cost(
        &mut self,
        bytes: u64,
        mutable: bool,
        data_name: XorName,
        msg_id: MessageId,
        origin: SrcLocation,
        // ) -> NodeDuties {
        //     let result = if bytes == 0 {
        //         Err(sn_messaging::client::Error::InvalidOperation(
        //             "Cannot store 0 bytes".to_string(),
        //         ))
        //     } else {
        //         let store_cost = self.rate_limit.from(bytes).await;
        //         info!("StoreCost for {:?} bytes: {}", bytes, store_cost);
        //         Ok((bytes, data_name, store_cost))
        //     };

        //     let response = NodeDuty::Send(OutgoingMsg {
        //         msg: MsgType::Client(ClientMsg::Process(ProcessMsg::QueryResponse {
    ) -> NodeDuty {
        let result = self
            .store_cost(bytes, mutable, data_name)
            .await
            .map_err(|e| sn_messaging::client::Error::InvalidOperation(e.to_string()));
        NodeDuty::Send(OutgoingMsg {
            msg: MsgType::Client(ClientMsg::Process(ProcessMsg::QueryResponse {
                response: QueryResponse::GetStoreCost(result),
                id: MessageId::in_response_to(&msg_id),
                correlation_id: msg_id,
            })),
            section_source: false, // strictly this is not correct, but we don't expect responses to a response..
            dst: origin.to_dst(),
            aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
        })
    }

    // /// Makes sure the payment contained
    // /// within a data write, is credited
    // /// to the section funds.
    // pub async fn process_payment(&mut self, msg: ProcessMsg, origin: EndUser) -> Result<NodeDuty> {
    //     let msg_id = msg.id();
    //     let (payment, num_bytes, dst_address, data_cmd) = match msg {
    //         ProcessMsg::Cmd {
    //             cmd: Cmd::Data { payment, cmd },
    //             ..
    //         } => (
    //             payment,
    //             utils::serialise(&cmd)?.len() as u64,
    //             cmd.dst_address(),
    //             cmd,
    //         ),
    //         _ => return Ok(NodeDuty::NoOp),
    //     };
    async fn store_cost(&self, bytes: u64, mutable: bool, data_name: XorName) -> Result<StoreCost> {
        if bytes == 0 {
            Err(Error::InvalidOperation("Cannot store 0 bytes".to_string()))
        } else {
            let (store_cost, nodes) = if mutable {
                let elder_store_cost =
                    Token::from_nano(2 * self.rate_limit.from(bytes).await.as_nano());

                let elders = self.rate_limit.get_elders().await;
                (elder_store_cost, elders)
            } else {
                let adult_store_cost = self.rate_limit.from(bytes).await;
                let adults = self.rate_limit.get_chunk_holder_adults(&data_name).await;
                (adult_store_cost, adults)
            };

            info!(
                "StoreCost for {:?} bytes: {} (mutable: {})",
                bytes, store_cost, mutable
            );

            let wallets = self
                .node_wallets()
                .iter()
                .filter_map(|(key, value)| {
                    if nodes.contains(key) {
                        Some((*key, *value))
                    } else {
                        None
                    }
                })
                .collect();

            let payable: BTreeMap<_, _> = distribute_rewards(store_cost, wallets)
                .iter()
                .map(|(_, (_, key, amount))| (*key, *amount))
                .collect();

            Ok(StoreCost {
                bytes,
                data_name,
                payable,
            })
        }
    }

    /// Makes sure the payment contained
    /// within a data write, is credited
    /// to the nodes.
    pub async fn process_payment(&mut self, msg: ProcessMsg, origin: EndUser) -> Result<NodeDuty> {
        let msg_id = msg.id();

        let (payment, data_cmd, client_signed) = match self.validate_payment(msg).await {
            Ok(result) => result,
            Err(e) => {
                return Ok(NodeDuty::Send(OutgoingMsg {
                    msg: MsgType::Client(ClientMsg::Process(ProcessMsg::CmdError {
                        error: CmdError::Payment(PaymentError(e.to_string())),
                        id: MessageId::in_response_to(&msg_id),
                        correlation_id: msg_id,
                    })),
                    section_source: false, // strictly this is not correct, but we don't expect responses to a response..
                    dst: SrcLocation::EndUser(origin).to_dst(),
                    aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
                }));
            }
        };

        let inputs_belonging_to_mints = payment
            .transaction
            .inputs
            .iter()
            .filter(|dbc| is_close(dbc.name()))
            .map(|dbc| dbc.name())
            .collect();

        match self
            .mint
            .reissue(payment.clone(), inputs_belonging_to_mints)
        {
            Ok((tx, tx_sigs)) => {
                // TODO: store these somewhere, for nodes to claim
                let _output_dbcs: Vec<_> = payment
                    .transaction
                    .outputs
                    .into_iter()
                    .map(|content| Dbc {
                        content,
                        transaction: tx.clone(),
                        transaction_sigs: tx_sigs.clone(),
                    })
                    .collect();

                info!("Payment: forwarding data..");
                Ok(NodeDuty::Send(OutgoingMsg {
                    msg: MsgType::Node(NodeMsg::NodeCmd {
                        cmd: NodeCmd::Metadata {
                            cmd: data_cmd.clone(),
                            client_signed,
                            origin,
                        },
                        id: MessageId::in_response_to(&msg_id),
                    }),
                    section_source: true, // i.e. errors go to our section
                    dst: DstLocation::Section(data_cmd.dst_address()),
                    aggregation: Aggregation::AtDestination,
                }))
            }
            Err(e) => {
                warn!("Payment failed: {:?}", e);
                Ok(NodeDuty::Send(OutgoingMsg {
                    msg: MsgType::Client(ClientMsg::Process(ProcessMsg::CmdError {
                        error: CmdError::Payment(e.into()),
                        id: MessageId::in_response_to(&msg_id),
                        correlation_id: msg_id,
                    })),
                    section_source: false, // strictly this is not correct, but we don't expect responses to an error..
                    dst: SrcLocation::EndUser(origin).to_dst(),
                    aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
                }))
            }
        }
    }

    async fn validate_payment(
        &self,
        msg: ProcessMsg,
    ) -> Result<(MintRequest, DataCmd, ClientSigned)> {
        let (payment, num_bytes, data_cmd, client_signed) = match msg {
            ProcessMsg::Cmd {
                cmd: Cmd::Data { payment, cmd },
                client_signed,
                ..
            } => (
                payment,
                utils::serialise(&cmd)?.len() as u64,
                cmd,
                client_signed,
            ),
            _ => {
                return Err(Error::InvalidOperation(
                    "Payment is only needed for data writes.".to_string(),
                ))
            }
        };
        let mutable = matches!(data_cmd, DataCmd::Blob(_));
        // calculate current store cost given the parameters
        let store_cost = match self
            .store_cost(num_bytes, mutable, data_cmd.dst_address())
            .await
        {
            Ok(store_cost) => store_cost,
            Err(e) => return Err(e),
        };
        let total_cost = Token::from_nano(store_cost.payable.values().map(|v| v.as_nano()).sum());
        if total_cost > payment.amount() {
            return Err(Error::InvalidOperation(format!(
                "Too low payment: {}, expected: {}",
                payment.amount(),
                total_cost
            )));
        }
        // verify that each dbc amount is for an existing node,
        // and that the amount is proportional to the its age.
        for out in &payment.transaction.outputs {
            // TODO: let node_wallet = out.owner_key;
            let node_wallet = get_random_pk();
            match store_cost.payable.get(&node_wallet) {
                Some(expected_amount) => {
                    if expected_amount.as_nano() > out.amount {
                        return Err(Error::InvalidOperation(format!(
                            "Too low payment for {}: {}, expected {}",
                            node_wallet,
                            expected_amount,
                            Token::from_nano(out.amount),
                        )));
                    }
                }
                None => return Err(Error::InvalidOwner(node_wallet)),
            }
        }

        info!(
            "Payment: OK. (Store cost: {}, paid amount: {}.)",
            total_cost,
            payment.amount()
        );

        Ok((payment, data_cmd, client_signed))
    }
}

fn is_close(_hash: [u8; 32]) -> bool {
    true
}

fn get_random_pk() -> PublicKey {
    PublicKey::from(bls::SecretKey::random().public_key())
}

trait Amount {
    fn amount(&self) -> Token;
}

impl Amount for MintRequest {
    fn amount(&self) -> Token {
        Token::from_nano(self.transaction.inputs.iter().map(|dbc| dbc.amount()).sum())
    }
}
