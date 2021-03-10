// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::collections::BTreeSet;

use crate::{
    node::node_ops::{NetworkDuties, NodeMessagingDuty, OutgoingMsg},
    Error,
};
use crate::{Network, Result};
use log::error;
use sn_messaging::{client::Message, Aggregation, DstLocation, Itinerary, SrcLocation};
use sn_routing::XorName;

/// Sending of messages
/// to nodes and clients in the network.
#[derive(Clone)]
pub struct Messaging {
    network: Network,
}

impl Messaging {
    pub fn new(network: Network) -> Self {
        Self { network }
    }

    pub async fn process_messaging_duty(&self, duty: NodeMessagingDuty) -> Result<()> {
        use NodeMessagingDuty::*;
        match duty {
            Send(msg) => {
                self.send(msg).await?;
                Ok(())
            }
            SendToAdults { targets, msg } => {
                self.send_to_nodes(targets, &msg).await?;
                Ok(())
            }
            NoOp => Ok(()),
        }
    }

    async fn send(&self, msg: OutgoingMsg) -> Result<()> {
        let src = if msg.section_source {
            SrcLocation::Section(self.network.our_prefix().await.name())
        } else {
            SrcLocation::Node(self.network.our_name().await)
        };
        let itry = Itinerary {
            src,
            dst: msg.dst,
            aggregation: msg.aggregation,
        };
        let result = self
            .network
            .clone()
            .send_message(itry, msg.msg.serialize()?)
            .await;

        result.map_or_else(
            |err| {
                error!("Unable to send msg: {:?}", err);
                Err(Error::Logic(format!("Unable to send msg: {:?}", msg.id())))
            },
            |()| Ok(()),
        )
    }

    async fn send_to_nodes(&self, targets: BTreeSet<XorName>, msg: &Message) -> Result<()> {
        let name = self.network.our_name().await;
        let bytes = &msg.serialize()?;
        for target in targets {
            self.network
                .clone()
                .send_message(
                    Itinerary {
                        src: SrcLocation::Node(name),
                        dst: DstLocation::Node(XorName(target.0)),
                        aggregation: Aggregation::AtDestination,
                    },
                    bytes.clone(),
                )
                .await
                .map_or_else(
                    |err| {
                        error!("Unable to send Message to Peer: {:?}", err);
                    },
                    |()| {},
                );
        }
        Ok(())
    }
}