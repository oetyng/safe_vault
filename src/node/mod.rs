// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod adult_duties;
mod elder_duties;
mod node_duties;
mod node_ops;
pub mod state_db;

use crate::{
    chunk_store::UsedSpace,
    node::{
        node_duties::{NodeDuties, messaging::Messaging},
        node_ops::{NetworkDuties, NodeDuty},
        state_db::{get_age_group, store_age_group, store_new_reward_keypair, AgeGroup},

    },
    network_state::{AdultReader, NodeInteraction, NodeSigning},
    Config, Error, Network, NodeInfo, Result,
};
use bls::SecretKey;
use log::{error, info};
use sn_data_types::{PublicKey, ReplicaPublicKeySet, Signature, SignatureShare};
use sn_routing::{EventStream, MIN_AGE, Event as RoutingEvent};
use std::{
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};


// use bls::{PublicKeySet};
use ed25519_dalek::PublicKey as Ed25519PublicKey;
// use itertools::Itertools;
// use log::debug;
// use serde::Serialize;
// use sn_data_types::{PublicKey, Signature, SignatureShare};
// use sn_messaging::client::TransientElderKey;
use sn_routing::SectionChain;
// use std::{
//     collections::BTreeSet,
//     // net::SocketAddr,
//     path::{Path, PathBuf},
// };
use xor_name::{Prefix, XorName};


/// Main node struct.
pub struct Node {
    // duties: NodeDuties,
    messaging: Messaging,
    network_api: Network,
    network_events: EventStream,


    // old adult
    // prefix: Prefix,
    // node_name: XorName,
    // node_id: Ed25519PublicKey,
    // section_chain: SectionChain,
    // elders: Vec<(XorName, SocketAddr)>,
    // adult_reader: AdultReader,
    // node_signing: NodeSigning,

    //old elder
    prefix: Option<Prefix>,
    node_name: XorName,
    node_id: Ed25519PublicKey,
    key_index: usize,
    public_key_set: ReplicaPublicKeySet,
    sibling_public_key: Option<PublicKey>,
    section_chain: SectionChain,
    elders: Vec<(XorName, SocketAddr)>,
    adult_reader: AdultReader,
    interaction: NodeInteraction,
    node_signing: NodeSigning,
}

impl Node {
    /// Initialize a new node.
    pub async fn new(config: &Config) -> Result<Self> {
        /// TODO: STARTUP all things
        let root_dir_buf = config.root_dir()?;
        let root_dir = root_dir_buf.as_path();
        std::fs::create_dir_all(root_dir)?;

        let reward_key_task = async move {
            let res: Result<PublicKey>;
            match config.wallet_id() {
                Some(public_key) => {
                    res = Ok(PublicKey::Bls(state_db::pk_from_hex(public_key)?));
                }
                None => {
                    let secret = SecretKey::random();
                    let public = secret.public_key();
                    store_new_reward_keypair(root_dir, &secret, &public).await?;
                    res = Ok(PublicKey::Bls(public));
                }
            };
            res
        }.await;
        // let age_group_task = async move {
        //     let res: Result<AgeGroup>;
        //     if let Some(age_group) = get_age_group(&root_dir).await? {
        //         res = Ok(age_group)
        //     } else {
        //         // let age_group = Infant;
        //         store_age_group(root_dir, &age_group).await?;
        //         res = Ok(age_group)
        //     };
        //     res
        // };
        
        let reward_key = reward_key_task?;
        // let (reward_key, _age_group) = tokio::try_join!(reward_key_task, age_group_task)?;
        let (network_api, network_events) = Network::new(config).await?;

        let node_info = NodeInfo {
            genesis: config.is_first(),
            root_dir: root_dir_buf,
            used_space: UsedSpace::new(config.max_capacity()),
            reward_key,


          
        };

        // use AgeGroup::*;
        // let age = network_api.age().await;
        // let age_group = if !network_api.is_elder().await && age > MIN_AGE {
        //     Adult
        // } else {
        //     Infant
        // };


        let messaging = Messaging::new(network_api.clone());


        // TODO:  HERE SETUP ADULT/ELDEEER
        // let init_ops = match age_group {
        //     Infant => Ok(vec![]),
        //     Adult => Ok(NetworkDuties::from(node_ops::NodeDuty::AssumeAdultDuties)),
        //     Elder => Err(Error::Logic("Unimplemented".to_string())),
        // };

        let node = Self {
            // duties: NodeDuties::new(node_info, network_api.clone()).await?,
            prefix: Some(network_api.our_prefix().await),
            node_name: network_api.our_name().await,
            node_id: network_api.public_key().await,
            key_index: network_api.our_index().await?,
            public_key_set: network_api.public_key_set().await?,
            sibling_public_key: network_api.sibling_public_key().await,
            section_chain: network_api.section_chain().await,
            elders: network_api.our_elder_addresses().await,
            adult_reader: AdultReader::new(network_api.clone()),
            interaction: NodeInteraction::new(network_api.clone()),
            node_signing: NodeSigning::new(network_api.clone()),


            network_api,
            network_events,
            messaging,

        };

        // node.process_while_any().await;

        Ok(node)
    }

    /// Returns our connection info.
    pub async fn our_connection_info(&mut self) -> SocketAddr {
        self.network_api.our_connection_info().await
    }

    /// Returns whether routing node is in elder state.
    pub async fn is_elder(&self) -> bool {
        self.network_api.is_elder().await
    }

    /// Starts the node, and runs the main event loop.
    /// Blocks until the node is terminated, which is done
    /// by client sending in a `Command` to free it.
    pub async fn run(&mut self) -> Result<()> {
        //let info = self.network_api.our_connection_info().await;
        //info!("Listening for routing events at: {}", info);
        while let Some(event) = self.network_events.next().await {
            //info!("New event received from the Network: {:?}", event);
            self.process_network_event(event);

            // self.process_while_any(Ok(NetworkDuties::from(NodeDuty::ProcessNetworkEvent(
            //     event,
            // ))))
            // .await;
        }

        Ok(())
    }

    // async fn process_event(&self, event: RoutingEvent) -> Result<()> {
    //     self
    //     .process_network_event(event, &self.network_api)
    //     .await

    //     // Ok()
    // }

    pub async fn process_network_event(
        &mut self,
        event: RoutingEvent,
        // network_api: &Network,
    ) -> Result<()> {
        let network_api = &self.network_api;
        // use ElderDuty::*;
        //trace!("Processing Routing Event: {:?}", event);
        match event {
            RoutingEvent::Genesis => {
                Ok(())
                // Ok(NetworkDuties::from(NodeDuty::BeginFormingGenesisSection))
            },
            RoutingEvent::MemberLeft { name, age } => {
                // trace!("A node has left the section. Node: {:?}", name);
                // //self.log_node_counts().await;
                // Ok(NetworkDuties::from(ProcessLostMember {
                //     name: XorName(name.0),
                //     age,
                // }))
                Ok(())
            }
            RoutingEvent::MemberJoined {
                name,
                previous_name,
                age,
                ..
            } => {
                Ok(())
                // if Self::is_forming_genesis(network_api).await {
                //     // during formation of genesis we do not process this event
                //     return Ok(vec![]);
                // }

                // //info!("New member has joined the section");
                // //self.log_node_counts().await;
                // if let Some(prev_name) = previous_name {
                //     trace!("The new member is a Relocated Node");
                //     let first = NetworkDuty::from(ProcessRelocatedMember {
                //         old_node_id: XorName(prev_name.0),
                //         new_node_id: XorName(name.0),
                //         age,
                //     });

                //     // Switch joins_allowed off a new adult joining.
                //     //let second = NetworkDuty::from(SwitchNodeJoin(false));
                //     Ok(vec![first]) // , second
                // } else {
                //     //trace!("New node has just joined the network and is a fresh node.",);
                //     Ok(NetworkDuties::from(ProcessNewMember(XorName(name.0))))
                // }
            }
            RoutingEvent::ClientMessageReceived { msg, user } => {
                info!("Received client message: {:8?}\n Sent from {:?}", msg, user);
                Ok(())
                
                // self.analysis.evaluate(
                //     *msg,
                //     SrcLocation::EndUser(user),
                //     DstLocation::Node(self.analysis.name()),
                // )
            }
            RoutingEvent::MessageReceived { content, src, dst } => {
                // info!(
                //     "Received network message: {:8?}\n Sent from {:?} to {:?}",
                //     HexFmt(&content),
                //     src,
                //     dst
                // );
                // self.analysis.evaluate(Message::from(content)?, src, dst)

                Ok(())
            }
            RoutingEvent::EldersChanged {
                key,
                elders,
                prefix,
                self_status_change,
                sibling_key,
            } => {
                // let mut duties: NetworkDuties = match self_status_change {
                //     NodeElderChange::None => vec![],
                //     NodeElderChange::Promoted => {
                //         return if Self::is_forming_genesis(network_api).await {
                //             Ok(NetworkDuties::from(NodeDuty::BeginFormingGenesisSection))
                //         } else {
                //             // After genesis section formation, any new Elder will be informed
                //             // by its peers of data required. 
                //             // It may also request this if missing.
                //             // For now we start with defaults
                            
                //             Ok(NetworkDuties::from(NodeDuty::CompleteTransitionToElder{
                //                 node_rewards: Default::default(),
                //                 section_wallet: WalletInfo { 
                //                     replicas:  network_api.public_key_set().await?,
                //                     history: ActorHistory{
                //                         credits: vec![],
                //                         debits: vec![]
                //                     }
                //                 },
                //                 user_wallets: Default::default()
                //             }))
                //         };
                //     }
                //     NodeElderChange::Demoted => NetworkDuties::from(NodeDuty::AssumeAdultDuties),
                // };

                // let mut sibling_pk = None;
                // if let Some(pk) = sibling_key {
                //     sibling_pk = Some(PublicKey::Bls(pk));
                // }

                // duties.push(NetworkDuty::from(NodeDuty::UpdateElderInfo {
                //     prefix,
                //     key: PublicKey::Bls(key),
                //     elders: elders.into_iter().map(|e| XorName(e.0)).collect(),
                //     sibling_key: sibling_pk,
                // }));

                // Ok(duties)

                Ok(())
            }
            RoutingEvent::Relocated { .. } => {
                // Check our current status
                let age = network_api.age().await;
                if age > MIN_AGE {
                    info!("Node promoted to Adult");
                    info!("Our Age: {:?}", age);
                    // return Ok(())
                    // Ok(NetworkDuties::from(NodeDuty::AssumeAdultDuties))
                } 
                Ok(())
                // else {
                //     info!("Our AGE: {:?}", age);
                //     Ok(vec![])
                // }
            }
            // Ignore all other events
            _ => Ok(()),
            // _ => Ok(vec![]),
        }
    }

    /// Keeps processing resulting node operations.
    // async fn process_while_any(&mut self, ops_vec: Result<NetworkDuties>) {
    //     let mut next_ops = ops_vec;

    //     while let Ok(ops) = next_ops {
    //         let mut pending_node_ops = Vec::new();

    //         if !ops.is_empty() {
    //             for duty in ops {
    //                 match self.duties.process(duty).await {
    //                     Ok(new_ops) => pending_node_ops.extend(new_ops),
    //                     Err(e) => self.handle_error(&e),
    //                 };
    //             }
    //             next_ops = Ok(pending_node_ops);
    //         } else {
    //             break;
    //         }
    //     }
    // }

    fn handle_error(&self, err: &Error) {
        use std::error::Error;
        info!("unimplemented: Handle errors.. {}", err);

        if let Some(source) = err.source() {
            error!("Source of error: {:?}", source);
        }
    }
}

impl Display for Node {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "Node")
    }
}
