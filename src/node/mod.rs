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
    event_mapping::{map_routing_event, LazyError, Mapping, MsgContext},
    network::Network,
    node_ops::NodeDuty,
    state::State,
    state_db::store_new_reward_keypair,
    Config, Error, Result,
};
use bls::SecretKey;
use handle::DutyHandler;
use interaction::register_wallet;
use log::{error, info};
use sn_data_types::PublicKey;
use sn_routing::EventStream;
use sn_routing::{Prefix, XorName};
use std::{
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};

/// Main node struct.
pub struct Node {
    network_api: Network,
    network_events: EventStream,
    state: State,
}

impl Node {
    /// Initialize a new node.
    pub async fn new(config: &Config) -> Result<Self> {
        let root_dir = config.root_dir()?;
        let root_dir_path = root_dir.as_path();
        std::fs::create_dir_all(root_dir_path)?;

        let reward_key = async move {
            let res: Result<PublicKey>;
            match config.wallet_id() {
                Some(public_key) => {
                    res = Ok(PublicKey::Bls(crate::state_db::pk_from_hex(public_key)?));
                }
                None => {
                    let secret = SecretKey::random();
                    let public = secret.public_key();
                    store_new_reward_keypair(root_dir_path, &secret, &public).await?;
                    res = Ok(PublicKey::Bls(public));
                }
            };
            res
        }
        .await?;

        let (network_api, network_events) = Network::new(config).await?;

        let state = State::new(root_dir, &network_api, config.max_capacity(), reward_key).await?;

        messaging::send(register_wallet(&network_api, &state).await, &network_api).await?;

        let node = Self {
            network_api,
            network_events,
            state,
        };

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
            let network_api = self.network_api.clone();
            let state = self.state.clone();
            let _ = tokio::spawn(async move {
                match map_routing_event(event, &network_api).await {
                    Mapping::Ok { op, ctx } => process_while_any(network_api, state, op, ctx).await,
                    Mapping::Error(error) => handle_error(error),
                }
            })
            .await;
        }

        Ok(())
    }
}

/// Keeps processing resulting node operations.
async fn process_while_any(
    network_api: Network,
    state: State,
    op: NodeDuty,
    ctx: Option<MsgContext>,
) {
    let mut next_ops = vec![op];
    let mut duty_handler = DutyHandler { network_api, state };

    while !next_ops.is_empty() {
        let mut pending_node_ops: Vec<NodeDuty> = vec![];
        for duty in next_ops {
            // TODO: additional tasks spawning around intensive tasks, ie sign/verify
            // and/or for each new node duty
            match duty_handler.handle(duty).await {
                Ok(new_ops) => pending_node_ops.extend(new_ops),
                Err(e) => try_handle_error(e, ctx.clone()),
            }
        }
        next_ops = pending_node_ops;
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
