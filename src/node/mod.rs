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
    event_mapping::{map_routing_event, LazyError, Mapping, MsgContext},
    metadata::Metadata,
    network::Network,
    node_ops::NodeDuty,
    section_funds::SectionFunds,
    state_db::{get_reward_pk, store_new_reward_keypair},
    transfers::Transfers,
    Config, Error, Result,
};
use bls::SecretKey;
use log::{error, info};
use sn_data_types::{MapAddress, PublicKey, SequenceAddress};
use sn_routing::EventStream;
use sn_routing::{Prefix, XorName};
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
    // denotes if we are caught up
    is_caught_up: bool,
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

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
pub struct BlobDataExchange {
    /// Full Adults register
    pub full_adults: BTreeMap<String, String>,
    /// Blob holders register
    pub holders: BTreeMap<String, String>,
    /// Metadata register
    pub metadata: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
pub struct MapDataExchange(pub BTreeMap<MapAddress, sn_data_types::Map>);

#[derive(Serialize, Deserialize)]
pub struct SequenceDataExchange(pub BTreeMap<SequenceAddress, sn_data_types::Sequence>);

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
                Mapping::Error(error) => handle_error(error),
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
                match self.handle(duty).await {
                    Ok(new_ops) => pending_node_ops.extend(new_ops),
                    Err(e) => try_handle_error(e, ctx.clone()),
                };
            }
            next_ops = pending_node_ops;
        }
    }
}

fn handle_error(err: LazyError) {
    use std::error::Error;
    info!(
        "unimplemented: Handle errors. This should be return w/ lazyError to sender. {:?}",
        err
    );

    if let Some(source) = err.error.source() {
        error!("Source of error: {:?}", source);
    }
}

fn try_handle_error(err: Error, ctx: Option<MsgContext>) {
    use std::error::Error;
    if let Some(source) = err.source() {
        if let Some(_ctx) = ctx {
            info!(
                "unimplemented: Handle errors. This should be return w/ lazyError to sender. {:?}",
                err
            );
            error!("Source of error: {:?}", source);
        } else {
            error!(
                "Erroring without a msg context. Source of error: {:?}",
                source
            );
        }
    } else {
        info!("unimplemented: Handle errors. {:?}", err);
    }
}

impl Display for Node {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "Node")
    }
}
