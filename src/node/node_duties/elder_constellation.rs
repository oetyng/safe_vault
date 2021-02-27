// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::ElderDuties;
use crate::{NodeInfo, Result};

use crate::{node::node_ops::NetworkDuties, Error};
use log::{debug, info};
use sn_data_types::PublicKey;
use sn_routing::ElderKnowledge;

// we want a consistent view of the elder constellation

// when we have an ElderChange, underlying sn_routing will
// return the new key set on querying (caveats in high churn?)
// but we want a snapshot of the state to work with, before we use the new keys

// so, to correctly transition between keys, we need to not mix states,
// and keep a tidy order, i.e. use one constellation at a time.

///
pub struct ElderConstellation {
    duties: ElderDuties,
    pending_changes: Vec<ElderKnowledge>,
}

impl ElderConstellation {
    ///
    pub fn new(duties: ElderDuties) -> Self {
        Self {
            duties,
            pending_changes: vec![],
        }
    }

    ///
    pub fn duties(&mut self) -> &mut ElderDuties {
        &mut self.duties
    }

    ///
    pub async fn initiate_elder_change(
        &mut self,
        elder_knowledge: ElderKnowledge,
    ) -> Result<NetworkDuties> {
        let elder_state = self.duties.state();
        let new_section_key = PublicKey::Bls(elder_knowledge.section_key_set.public_key());
        debug!(
            ">> Prefix we have w/ elder change: {:?}",
            elder_knowledge.prefix
        );
        debug!(">> New section key w/ change {:?}", new_section_key);
        debug!(
            ">> IS THERE A SIBLING KEY??? {:?}",
            elder_knowledge.sibling_key
        );

        if new_section_key == elder_state.section_public_key()
            || self
                .pending_changes
                .iter()
                .any(|c| new_section_key == PublicKey::Bls(c.section_key_set.public_key()))
        {
            return Ok(vec![]);
        }

        info!(">>Elder change updates initiated");
        info!(
            ">>Pending changes len before {:?}",
            self.pending_changes.len()
        );
        self.pending_changes.push(elder_knowledge.clone());
        info!(
            ">>Pending changes len after {:?}",
            self.pending_changes.len()
        );

        // handle changes sequentially
        if self.pending_changes.len() > 1 {
            debug!(">> more changes so we return a vec?");
            return Ok(vec![]);
        }

        // 1. First we must update data section..
        let new_elder_state = elder_state.from(elder_knowledge)?;
        self.duties.initiate_elder_change(new_elder_state).await
    }

    ///
    pub async fn finish_elder_change(
        &mut self,
        node_info: &NodeInfo,
        previous_key: PublicKey,
        new_key: PublicKey,
    ) -> Result<NetworkDuties> {
        debug!(">> Finishing elder change!!");
        debug!(">>new key: {:?}", new_key);
        debug!(">>previous_key: {:?}", previous_key);

        if new_key == previous_key {
            debug!(">> !! same keys; IS AN error w/o the key transfer op.");
            return Err(Error::InvalidOperation(
                "new_key == previous_key".to_string(),
            ));
        }
        if self.pending_changes.is_empty() {
            debug!(">>  !! no changes, so return here empty vec");
            return Ok(vec![]);
        }

        let old_elder_state = self.duties.state().clone();
        if old_elder_state.section_public_key() != previous_key
            || new_key != PublicKey::Bls(self.pending_changes[0].section_key_set.public_key())
        {
            debug!(
                ">> !!old state key is not same as prev. ??  {:?}, {:?}",
                old_elder_state.section_public_key(),
                previous_key
            );
            debug!(
                ">> !! OR  new key isnt pending change {:?}, {:?}",
                self.pending_changes[0].section_key_set.public_key(),
                new_key
            );

            return Ok(vec![]);
        }

        debug!(">> past the noops");

        let mut ops: NetworkDuties = Vec::new();
        // pop the pending change..
        let change = self.pending_changes.remove(0);

        // 2. We must load _current_ elder state..
        let new_elder_state = old_elder_state.from(change.clone())?;
        // 3. And update key section with it.
        self.duties
            .finish_elder_change(node_info, new_elder_state.clone())
            .await?;

        debug!(">>Key section completed elder change update.");
        debug!(">>Elder change update completed.");

        // split section _after_ transition to new constellation
        if &change.prefix != old_elder_state.prefix() {
            info!(">>Split occurred");
            info!(">>New prefix is: {:?}", change.prefix);
            let duties = self.duties.split_section(change.prefix).await?;
            if !duties.is_empty() {
                ops.extend(duties)
            };
        }

        // if changes have queued up, make sure the queue is worked down
        if !self.pending_changes.is_empty() {
            let change = self.pending_changes.remove(0);
            debug!(">>Extending ops, NO sibling pk here... should there be?");
            ops.extend(self.initiate_elder_change(change).await?);
        }

        Ok(ops)
    }
}
