// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

// mod adult_duties;
// mod elder_duties;
// mod node_duties;
mod node_ops;
pub mod state_db;
use serde::Serialize;
mod genesis;
mod messaging;
use hex_fmt::HexFmt;

use crate::{
    chunk_store::UsedSpace,
    node::{
        genesis::GenesisStage, genesis::GenesisProposal, messaging::Messaging,
        // node_duties::{NodeDuties, GenesisStage, genesis::GenesisProposal, messaging::Messaging},
        node_ops::{NetworkDuties, NodeDuty, NodeMessagingDuty, OutgoingMsg},
        // TODO: strip out NodeMessagingDuty and just pass in msg
        state_db::{get_age_group, store_age_group, store_new_reward_keypair, AgeGroup},

    },
    network_state::{AdultReader, NodeInteraction, NodeSigning},
    Config, Error, Network, NodeInfo, Result,
};
use bls::SecretKey;
use log::{error, debug, info, trace};
use sn_data_types::{
    ActorHistory, Credit, NodeRewardStage, PublicKey,ReplicaPublicKeySet,Signature, SignatureShare, SignedCredit, Token,
    TransferPropagated, WalletInfo,
};
use sn_routing::{EventStream, MIN_AGE, Event as RoutingEvent};
use std::{
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};
use std::collections::BTreeMap;

// use bls::{PublicKeySet};
use ed25519_dalek::PublicKey as Ed25519PublicKey;
// use itertools::Itertools;
// use log::debug;
// use serde::Serialize;
// use sn_data_types::{PublicKey, Signature, SignatureShare};
use sn_messaging::{client::TransientElderKey, DstLocation, client::{Message, MsgSender, NodeCmd, NodeSystemCmd}, MessageId, Aggregation};
use sn_routing::SectionChain;
// use std::{
//     collections::BTreeSet,
//     // net::SocketAddr,
//     path::{Path, PathBuf},
// };

use sn_routing::{XorName, Prefix, ELDER_SIZE as GENESIS_ELDER_COUNT};


/// Main node struct.
pub struct Node {
    // duties: NodeDuties,
    messaging: Messaging,
    network_api: Network,
    network_events: EventStream,
    node_info: NodeInfo,

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
    // key_index: usize,
    // public_key_set: ReplicaPublicKeySet,
    sibling_public_key: Option<PublicKey>,
    section_chain: SectionChain,
    elders: Vec<(XorName, SocketAddr)>,
    adult_reader: AdultReader,
    interaction: NodeInteraction,
    node_signing: NodeSigning,

    genesis_stage: GenesisStage
}

impl Node {
    /// Initialize a new node.
    pub async fn new(config: &Config) -> Result<Self> {
        /// TODO: STARTUP all things
        let root_dir_buf = config.root_dir()?;
        let root_dir = root_dir_buf.as_path();
        std::fs::create_dir_all(root_dir)?;

        debug!("NEW NODE");
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
        debug!("NEW NODE after reward key");
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

        debug!("NEW NODE after messaging");

        // TODO:  HERE SETUP ADULT/ELDEEER
        // let init_ops = match age_group {
        //     Infant => Ok(vec![]),
        //     Adult => Ok(NetworkDuties::from(node_ops::NodeDuty::AssumeAdultDuties)),
        //     Elder => Err(Error::Logic("Unimplemented".to_string())),
        // };

        // let index = match network_api.our_index().await {
        //     Ok(index) => Ok(index),
        //     Err(error) => {
        //         error!("AAH INDEX: {:?}",error);

        //         Err(error)
        //     }
        // }?;
        
        let node = Self {
            // duties: NodeDuties::new(node_info, network_api.clone()).await?,
            prefix: Some(network_api.our_prefix().await),
            node_name: network_api.our_name().await,
            node_id: network_api.public_key().await,
            // key_index: index,
            // public_key_set: network_api.public_key_set().await?,
            sibling_public_key: network_api.sibling_public_key().await,
            section_chain: network_api.section_chain().await,
            elders: network_api.our_elder_addresses().await,
            adult_reader: AdultReader::new(network_api.clone()),
            interaction: NodeInteraction::new(network_api.clone()),
            node_signing: NodeSigning::new(network_api.clone()),

            node_info,

            network_api,
            network_events,
            messaging,

            genesis_stage: GenesisStage::None
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
            self.process_network_event(event).await?;

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

    /// Process any routing event
    pub async fn process_network_event(
        &mut self,
        event: RoutingEvent,
        // network_api: &Network,
    ) -> Result<()> {
        let network_api = &self.network_api;

        // use ElderDuty::*;
        trace!("Processing Routing Event: {:?}", event);
        match event {
            RoutingEvent::Genesis => {
                   
                     if !self.node_info.genesis {
                        return Err(Error::InvalidOperation(
                            "only genesis node can transition to Elder as Infant".to_string(),
                        ));
                    }
            
                    let is_genesis_section = self.network_api.our_prefix().await.is_empty();
                    let elder_count = self.network_api.our_elder_names().await.len();
                    let section_chain_len = self.network_api.section_chain().await.len();
                    // debug!(
                    //     "begin_transition_to_elder. is_genesis_section: {}, elder_count: {}, section_chain_len: {}",
                    //     is_genesis_section, elder_count, section_chain_len
                    // );
                    if is_genesis_section
                        && elder_count == GENESIS_ELDER_COUNT
                        && section_chain_len <= GENESIS_ELDER_COUNT
                    {
                        // this is the case when we are the GENESIS_ELDER_COUNT-th Elder!
                        debug!("threshold reached; proposing genesis!");
            
                        // let elder_state = ElderState::new(self.network_api.clone()).await?;
                        let genesis_balance = u32::MAX as u64 * 1_000_000_000;
                        let credit = Credit {
                            id: Default::default(),
                            amount: Token::from_nano(genesis_balance),
                            recipient: self.network_api.section_public_key().await.ok_or(Error::NoSectionPublicKey)?,
                            msg: "genesis".to_string(),
                        };
                        let mut signatures: BTreeMap<usize, bls::SignatureShare> = Default::default();
                        let credit_sig_share = self.sign_as_elder(&credit).await?;
                        let _ = signatures.insert(credit_sig_share.index, credit_sig_share.share.clone());
            
                        self.genesis_stage = GenesisStage::ProposingGenesis(GenesisProposal {
                            proposal: credit.clone(),
                            signatures,
                            pending_agreement: None,
                        });
            
                        let dst = DstLocation::Section(credit.recipient.into());


                        self.messaging.process_messaging_duty(
                            NodeMessagingDuty::Send(OutgoingMsg {
                                msg: Message::NodeCmd {
                                    cmd: NodeCmd::System(NodeSystemCmd::ProposeGenesis {
                                        credit,
                                        sig: credit_sig_share,
                                    }),
                                    id: MessageId::new(),
                                    target_section_pk: None,
                                },
                                dst,
                                section_source: false, // sent as single node
                                aggregation: Aggregation::None,
                            })
                            
                        ).await?;

                        
                        // return Ok(NetworkDuties::from());
                    } else if is_genesis_section
                        && elder_count < GENESIS_ELDER_COUNT
                        && section_chain_len <= GENESIS_ELDER_COUNT
                    {
                        debug!("AwaitingGenesisThreshold!");
                        self.genesis_stage = GenesisStage::AwaitingGenesisThreshold;
                        // return Ok(vec![]);
                    } 
                    else {

                        debug!("HITTING GENESIS ELSE FOR SOME REASON....");
                        // Err(Error::InvalidOperation(
                        //     "Only for genesis formation".to_string(),
                        // ))
                    }
                
                Ok(())
                // Ok(NetworkDuties::from(NodeDuty::BeginFormingGenesisSection))
            },
            RoutingEvent::MemberLeft { name, age } => {
                trace!("A node has left the section. Node: {:?}", name);
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
                if self.is_forming_genesis().await {
                    // during formation of genesis we do not process this event
                    debug!("Forming genesis so ignore new member");
                    return Ok(())
                }

                info!("New member has joined the section");
                Ok(())
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
                info!(
                    "Received network message: {:8?}\n Sent from {:?} to {:?}",
                    HexFmt(&content),
                    src,
                    dst
                );
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
                trace!("Elders changed event!");
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
        info!("unimplemented: Handle errors. This should be return w/ lazyError to sender. {}", err);

        if let Some(source) = err.source() {
            error!("Source of error: {:?}", source);
        }
    }

    /// TODO rename to SignWithAuthority?
    pub async fn sign_as_elder<T: Serialize>(&self, data: &T) -> Result<SignatureShare> {
        let share = self
            .node_signing
            .sign_as_elder(data, 
                &self.network_api.public_key_set().await?
                .public_key()).await?;
        Ok(SignatureShare {
            share,
            index: self.network_api.our_index().await?,
        })
    }

    /// Are we forming the genesis?
    async fn is_forming_genesis(&self) -> bool {
        let is_genesis_section = self.network_api.our_prefix().await.is_empty();
        let elder_count = self.network_api.our_elder_names().await.len();
        let section_chain_len = self.network_api.section_chain().await.len();
        is_genesis_section
            && elder_count <= GENESIS_ELDER_COUNT
            && section_chain_len <= GENESIS_ELDER_COUNT
    }
}



impl Display for Node {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "Node")
    }
}
