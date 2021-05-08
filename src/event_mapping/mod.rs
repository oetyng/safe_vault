// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod map_msg;

use super::node_ops::NodeDuty;
use crate::network::Network;
use log::{debug, error, info};
use map_msg::{map_node_msg, match_user_sent_msg};
use sn_data_types::PublicKey;
use sn_messaging::{client::Message, SrcLocation};
use sn_routing::XorName;
use sn_routing::{Event as RoutingEvent, NodeElderChange};
use std::{thread::sleep, time::Duration};

#[derive(Debug)]
pub enum Mapping {
    Ok {
        op: NodeDuty,
        ctx: Option<MsgContext>,
    },
    Error(LazyError),
}

#[derive(Debug, Clone)]
pub enum MsgContext {
    Msg { msg: Message, src: SrcLocation },
    Bytes { msg: bytes::Bytes, src: SrcLocation },
}

#[derive(Debug)]
pub struct LazyError {
    pub msg: MsgContext,
    pub error: crate::Error,
}

/// Process any routing event
pub async fn map_routing_event(event: RoutingEvent, network_api: &Network) -> Mapping {
    match event {
        RoutingEvent::MessageReceived {
            content, src, dst, ..
        } => {
            let msg = match Message::from(content.clone()) {
                Ok(msg) => msg,
                Err(error) => {
                    return Mapping::Error(LazyError {
                        msg: MsgContext::Bytes { msg: content, src },
                        error: crate::Error::Message(error),
                    })
                }
            };

            map_node_msg(msg, src, dst)
        }
        RoutingEvent::ClientMessageReceived { msg, user } => {
            info!(
                "ClientMessageReceived {{ msg: {:?}, user: {:?} }}",
                msg, user
            );
            match_user_sent_msg(*msg, user)
        }
        RoutingEvent::SectionSplit {
            elders,
            sibling_elders,
            self_status_change,
        } => {
            let newbie = match self_status_change {
                NodeElderChange::None => false,
                NodeElderChange::Promoted => true,
                NodeElderChange::Demoted => {
                    error!("This should be unreachable, as there would be no demotions of Elders during a split.");
                    return Mapping::Ok {
                        op: NodeDuty::NoOp,
                        ctx: None,
                    };
                }
            };
            Mapping::Ok {
                op: NodeDuty::SectionSplit {
                    our_prefix: elders.prefix,
                    our_key: PublicKey::from(elders.key),
                    our_new_elders: elders.added,
                    their_new_elders: sibling_elders.added,
                    sibling_key: PublicKey::from(sibling_elders.key),
                    newbie,
                },
                ctx: None,
            }
        }
        RoutingEvent::EldersChanged {
            elders,
            self_status_change,
        } => {
            let first_section = network_api.our_prefix().await.is_empty();
            let first_elder = network_api.our_elder_names().await.len() == 1;
            if first_section && first_elder {
                return Mapping::Ok {
                    op: NodeDuty::Genesis,
                    ctx: None,
                };
            }
            match self_status_change {
                NodeElderChange::None => {
                    if !network_api.is_elder().await {
                        return Mapping::Ok {
                            op: NodeDuty::NoOp,
                            ctx: None,
                        };
                    }
                    // sync to others if we are elder
                    // -- ugly temporary until fixed in routing --
                    let mut sanity_counter = 0_i32;
                    while sanity_counter < 240 {
                        match network_api.our_public_key_set().await {
                            Ok(pk_set) => {
                                if elders.key == pk_set.public_key() {
                                    break;
                                } else {
                                    debug!("******Elders changed, we are still Elder but we seem to be lagging the DKG...");
                                }
                            }
                            Err(e) => {
                                debug!(
                                    "******Elders changed, should NOT be an error here...! ({:?})",
                                    e
                                );
                                sanity_counter += 1;
                            }
                        }
                        sleep(Duration::from_millis(500))
                    }
                    // -- ugly temporary until fixed in routing --

                    Mapping::Ok {
                        op: NodeDuty::EldersChanged {
                            our_prefix: elders.prefix,
                            our_key: PublicKey::from(elders.key),
                            new_elders: elders.added,
                            newbie: false,
                        },
                        ctx: None,
                    }
                }
                NodeElderChange::Promoted => {
                    // -- ugly temporary until fixed in routing --
                    let mut sanity_counter = 0_i32;
                    while network_api.our_public_key_set().await.is_err() {
                        if sanity_counter > 240 {
                            debug!("******Elders changed, we were promoted, but no key share found, so skip this..");
                            return Mapping::Ok {
                                op: NodeDuty::NoOp,
                                ctx: None,
                            };
                        }
                        sanity_counter += 1;
                        debug!("******Elders changed, we are promoted, but still no key share..");
                        sleep(Duration::from_millis(500))
                    }
                    // -- ugly temporary until fixed in routing --

                    Mapping::Ok {
                        op: NodeDuty::EldersChanged {
                            our_prefix: elders.prefix,
                            our_key: PublicKey::from(elders.key),
                            new_elders: elders.added,
                            newbie: true,
                        },
                        ctx: None,
                    }
                }
                NodeElderChange::Demoted => Mapping::Ok {
                    op: NodeDuty::LevelDown,
                    ctx: None,
                },
            }
        }
        RoutingEvent::MemberLeft { name, age } => {
            info!("MemberLeft {{ name: {}, age: {} }}", name, age);
            Mapping::Ok {
                op: NodeDuty::ProcessLostMember {
                    name: XorName(name.0),
                    age,
                },
                ctx: None,
            }
        }
        RoutingEvent::MemberJoined {
            name,
            age,
            previous_name,
            ..
        } => {
            info!("MemberJoined {{ name: {}, age: {} }}", name, age);
            let op = if previous_name.is_some() {
                // Switch joins_allowed off after a new adult joined.
                NodeDuty::SetNodeJoinsAllowed(false)
            } else if network_api.our_prefix().await.is_empty() {
                NodeDuty::NoOp
            } else {
                NodeDuty::SetNodeJoinsAllowed(false)
            };
            Mapping::Ok { op, ctx: None }
        }
        RoutingEvent::Relocated { .. } => {
            let age = network_api.age().await;
            info!("Relocated {{ our_age: {:?} }}", age);
            Mapping::Ok {
                op: NodeDuty::NoOp,
                ctx: None,
            }
        }
        RoutingEvent::AdultsChanged {
            remaining,
            added,
            removed,
        } => {
            info!(
                "AdultsChanged {{ prefix: {:?}, adults: {}, }}",
                network_api.our_prefix().await,
                remaining.len() + added.len(),
            );
            Mapping::Ok {
                op: NodeDuty::AdultsChanged {
                    remaining,
                    added,
                    removed,
                },
                ctx: None,
            }
        }
        // Ignore all other events
        _ => Mapping::Ok {
            op: NodeDuty::NoOp,
            ctx: None,
        },
    }
}
