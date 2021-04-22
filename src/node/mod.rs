// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod handle;
mod interaction;
mod member_churn;
mod messaging;
mod split;

use crate::{
    chunk_store::UsedSpace,
    chunks::Chunks,
    error::convert_to_error_message,
    event_mapping::{map_routing_event, Mapping, MsgContext},
    metadata::Metadata,
    network::Network,
    node_ops::{NodeDuty, OutgoingLazyError, OutgoingMsg},
    section_funds::SectionFunds,
    state_db::{get_reward_pk, store_new_reward_keypair},
    transfers::Transfers,
    Config, Error, Result,
};
use bls::SecretKey;
use log::{error, info};
use sn_data_types::PublicKey;
use sn_messaging::client::{Error as ErrorMessage, Message, ProcessMsg, ProcessingError};
use sn_routing::{EventStream, Prefix, XorName};
use std::path::{Path, PathBuf};
use std::{
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};

/// Static info about the node.
#[derive(Clone)]
pub struct NodeInfo {
    ///
    pub root_dir: PathBuf,
    /// The key used by the node to receive earned rewards.
    pub reward_key: PublicKey,
}

impl NodeInfo {
    ///
    pub fn path(&self) -> &Path {
        self.root_dir.as_path()
    }
}

struct AdultRole {
    // immutable chunks
    chunks: Chunks,
}

struct ElderRole {
    // data operations
    meta_data: Metadata,
    // transfers
    transfers: Transfers,
    // reward payouts
    section_funds: SectionFunds,
    // denotes if we received initial sync
    received_initial_sync: bool,
}

#[allow(clippy::large_enum_variant)]
enum Role {
    Adult(AdultRole),
    Elder(ElderRole),
}

impl Role {
    fn as_adult(&self) -> Result<&AdultRole> {
        match self {
            Self::Adult(adult) => Ok(adult),
            _ => Err(Error::NotAnAdult),
        }
    }

    fn as_adult_mut(&mut self) -> Result<&mut AdultRole> {
        match self {
            Self::Adult(adult) => Ok(adult),
            _ => Err(Error::NotAnAdult),
        }
    }

    fn as_elder(&self) -> Result<&ElderRole> {
        match self {
            Self::Elder(elder) => Ok(elder),
            _ => Err(Error::NotAnElder),
        }
    }

    fn as_elder_mut(&mut self) -> Result<&mut ElderRole> {
        match self {
            Self::Elder(elder) => Ok(elder),
            _ => Err(Error::NotAnElder),
        }
    }
}

/// Main node struct.
pub struct Node {
    network_api: Network,
    network_events: EventStream,
    node_info: NodeInfo,
    used_space: UsedSpace,
    role: Role,
}

impl Node {
    /// Initialize a new node.
    /// https://github.com/rust-lang/rust-clippy/issues?q=is%3Aissue+is%3Aopen+eval_order_dependence
    #[allow(clippy::eval_order_dependence)]
    pub async fn new(config: &Config) -> Result<Self> {
        // TODO: STARTUP all things
        let root_dir_buf = config.root_dir()?;
        let root_dir = root_dir_buf.as_path();
        std::fs::create_dir_all(root_dir)?;

        let reward_key = match get_reward_pk(root_dir).await? {
            Some(public_key) => PublicKey::Bls(public_key),
            None => {
                let secret = SecretKey::random();
                let public = secret.public_key();
                store_new_reward_keypair(root_dir, &secret, &public).await?;
                PublicKey::Bls(public)
            }
        };

        let (network_api, network_events) = Network::new(root_dir, config).await?;

        let node_info = NodeInfo {
            root_dir: root_dir_buf,
            reward_key,
        };

        let used_space = UsedSpace::new(config.max_capacity());

        let node = Self {
            role: Role::Adult(AdultRole {
                chunks: Chunks::new(node_info.root_dir.as_path(), used_space.clone()).await?,
            }),
            node_info,
            used_space,
            network_api,
            network_events,
        };

        messaging::send(node.register_wallet().await, &node.network_api).await?;

        Ok(node)
    }

    /// Returns our connection info.
    pub fn our_connection_info(&mut self) -> SocketAddr {
        self.network_api.our_connection_info()
    }

    /// Returns our name.
    pub async fn our_name(&mut self) -> XorName {
        self.network_api.our_name().await
    }

    /// Returns our prefix.
    pub async fn our_prefix(&mut self) -> Prefix {
        self.network_api.our_prefix().await
    }

    /// Starts the node, and runs the main event loop.
    /// Blocks until the node is terminated, which is done
    /// by client sending in a `Command` to free it.
    pub async fn run(&mut self) -> Result<()> {
        while let Some(event) = self.network_events.next().await {
            // tokio spawn should only be needed around intensive tasks, ie sign/verify
            match map_routing_event(event, &self.network_api).await {
                Mapping::Ok { op, ctx } => self.process_while_any(op, ctx).await,
                Mapping::Error { msg, error } => {
                    let duty = try_handle_error(error, Some(msg));
                    self.process_while_any(duty, None).await;
                }
            }
        }

        Ok(())
    }

    /// Keeps processing resulting node operations.
    async fn process_while_any(&mut self, op: NodeDuty, ctx: Option<MsgContext>) {
        let mut next_ops = vec![op];

        while !next_ops.is_empty() {
            let mut pending_node_ops: Vec<NodeDuty> = vec![];
            for duty in next_ops {
                match self.handle(duty, &ctx).await {
                    Ok(new_ops) => pending_node_ops.extend(new_ops),
                    Err(e) => {
                        let new_op = try_handle_error(e, ctx.clone());
                        pending_node_ops.push(new_op)
                    }
                };
            }
            next_ops = pending_node_ops;
        }
    }
}

fn get_dst_from_src(src: SrcLocation) -> DstLocation {
    match src {
        SrcLocation::EndUser(user) => DstLocation::EndUser(user),
        SrcLocation::Node(node) => DstLocation::Node(node),
        SrcLocation::Section(section) => DstLocation::Section(section),
    }
}

fn try_handle_error(err: Error, ctx: Option<MsgContext>) -> NodeDuty {
    use std::error::Error;
    warn!("Error being handled by node: {:?}", err);
    if let Some(source) = err.source() {
        warn!("Source: {:?}", source);
    }
    if let Some(ctx) = ctx {
        match ctx {
            // The message that triggered this error
            MsgContext::Msg { msg, src } => {
                warn!("Sending in response to a message: {:?}", msg);
                let dst = get_dst_from_src(src);
                match msg {
                    Message::Process(msg) => {
                        let error_message: ErrorMessage = match convert_to_error_message(err) {
                            Ok(err) => err,
                            Err(error) => {
                                error!("Problem handling error: {:?}", error);
                                return NodeDuty::NoOp;
                            }
                        };

                        NodeDuty::SendError(OutgoingLazyError {
                            msg: msg.create_processing_error(Some(error_message)),
                            dst,
                        })
                    }
                    Message::ProcessingError(err) => {
                        // TODO: handle error as a result of handling processing error...
                        NodeDuty::NoOp
                    }
                }
            }
            // An error decoding a message
            MsgContext::Bytes { msg, src } => {
                warn!("Error decoding msg bytes, sent from {:?}", src);
                let dst = get_dst_from_src(src);

                NodeDuty::SendError(OutgoingLazyError {
                    msg: ProcessingError {
                        reason: Some(ErrorMessage::Serialization(
                            "Could not deserialize Message at node".to_string(),
                        )),
                        source_message: None,
                        id: MessageId::new(),
                    },
                    dst,
                })
            }
        }
    } else {
        NodeDuty::NoOp
    }
}

impl Display for Node {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "Node")
    }
}
