// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::role::{ElderRole, Role};
use crate::{
    capacity::{AdultsStorageInfo, Capacity, CapacityReader, CapacityWriter, RateLimit},
    metadata::{adult_reader::AdultReader, Metadata},
    node_ops::NodeDuty,
    payments::{reward_wallets::RewardWallets, Payments},
    Node, Result,
};
use log::info;
use sn_data_types::{NodeAge, PublicKey};
use sn_dbc::Mint;
use sn_messaging::client::DataExchange;
use sn_routing::XorName;
use std::collections::BTreeMap;

impl Node {
    /// Level up a newbie to an oldie on promotion
    pub async fn level_up(&mut self) -> Result<()> {
        self.used_space.reset().await?;

        let adult_storage_info = AdultsStorageInfo::new();
        let adult_reader = AdultReader::new(self.network_api.clone());
        let capacity_reader = CapacityReader::new(adult_storage_info.clone(), adult_reader.clone());
        let capacity_writer = CapacityWriter::new(adult_storage_info.clone(), adult_reader.clone());
        let capacity = Capacity::new(capacity_reader.clone(), capacity_writer);

        //
        // start handling metadata

        let max_capacity = self.used_space.max_capacity().await;
        let meta_data =
            Metadata::new(&self.node_info.path(), max_capacity, capacity.clone()).await?;

        //
        // start handling payments
        let rate_limit = RateLimit::new(self.network_api.clone(), capacity_reader.clone());
        let node_wallets = RewardWallets::new(BTreeMap::<XorName, (NodeAge, PublicKey)>::new());
        let (mint, _dbc) = Mint::genesis(
            self.network_api.our_public_key_set().await?,
            crate::capacity::MAX_SUPPLY,
        );
        let payments = Payments::new(rate_limit, node_wallets, mint);

        self.role = Role::Elder(ElderRole {
            meta_data,
            payments,
            received_initial_sync: false,
        });

        Ok(())
    }

    /// Continue the level up and handle more responsibilities.
    pub async fn synch_state(
        &mut self,
        node_wallets: BTreeMap<XorName, (NodeAge, PublicKey)>,
        metadata: DataExchange,
    ) -> Result<NodeDuty> {
        let elder = self.role.as_elder_mut()?;

        if elder.received_initial_sync {
            info!("We are already received the initial sync from our section. Ignoring update");
            return Ok(NodeDuty::NoOp);
        }

        // --------- merge in provided node wallets ---------
        for (key, (age, wallet)) in &node_wallets {
            elder.payments.set_node_wallet(*key, *wallet, *age)
        }
        // --------- merge in provided metadata ---------
        elder.meta_data.update(metadata).await?;

        elder.received_initial_sync = true;

        let node_id = self.network_api.our_name().await;
        let no_wallet_found = node_wallets.get(&node_id).is_none();

        if no_wallet_found {
            info!(
                "Registering wallet of node: {} (since not found in received state)",
                node_id,
            );
            Ok(NodeDuty::Send(self.register_wallet().await))
        } else {
            Ok(NodeDuty::NoOp)
        }
    }
}
