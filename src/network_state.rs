// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

// we want a consistent view of the elder constellation

// when we have an ElderChange, underlying sn_routing will
// return the new key set on querying (caveats in high churn?)
// but we want a snapshot of the state to work with, before we use the new keys

// so, to correctly transition between keys, we need to not mix states,
// and keep a tidy order, i.e. use one constellation at a time.

use crate::{chunk_store::UsedSpace, Network, Result};
use bls::{PublicKeySet, PublicKeyShare};
use ed25519_dalek::PublicKey as Ed25519PublicKey;
use serde::Serialize;
use sn_data_types::{PublicKey, Signature, SignatureShare};
use sn_routing::{ElderKnowledge, SectionChain};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
use xor_name::{Prefix, XorName};

// we want a consistent view of the elder constellation
// when we have an ElderChange, underlying sn_routing will
// return the new key set in an event,
// together with a snapshot of state we work with
// to correctly transition between keys, we need to not mix states,
// and keep a tidy order, i.e. use one constellation at a time.

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
///
pub enum NodeState {
    ///
    Infant(Ed25519PublicKey),
    ///
    Adult(AdultState),
    ///
    Elder(ElderState),
}

impl NodeState {
    /// Static state
    pub fn node_id(&self) -> Ed25519PublicKey {
        match self {
            Self::Infant(id) => *id,
            Self::Adult(state) => state.node_id,
            Self::Elder(state) => state.node_id,
        }
    }

    ///
    pub fn node_name(&self) -> XorName {
        PublicKey::Ed25519(self.node_id()).into()
    }
}

#[derive(Clone)]
///
pub struct AdultState {
    node_id: Ed25519PublicKey,
    node_name: XorName,
    adult_reader: AdultReader,
    node_signing: NodeSigning,
}

impl AdultState {
    /// Takes a snapshot of current state
    /// https://github.com/rust-lang/rust-clippy/issues?q=is%3Aissue+is%3Aopen+eval_order_dependence
    #[allow(clippy::eval_order_dependence)]
    pub async fn new(
        node_id: Ed25519PublicKey,
        adult_reader: AdultReader,
        node_signing: NodeSigning,
    ) -> Result<Self> {
        Ok(Self {
            node_name: PublicKey::Ed25519(node_id).into(),
            node_id,
            adult_reader,
            node_signing,
        })
    }

    // ---------------------------------------------------
    // ----------------- STATIC STATE --------------------
    // ---------------------------------------------------

    /// Static state
    pub fn node_name(&self) -> XorName {
        self.node_name
    }

    /// Static state
    pub fn node_id(&self) -> Ed25519PublicKey {
        self.node_id
    }

    /// "Sort of" static; this is calling into routing layer
    /// but the underlying keys will not change.
    pub async fn sign_as_node<T: Serialize>(&self, data: &T) -> Result<Signature> {
        self.node_signing.sign_as_node(&data).await
    }
}

#[derive(Clone)]
///
pub struct ElderState {
    prefix: Prefix,
    node_name: XorName,
    node_id: Ed25519PublicKey,
    key_index: usize,
    section_key_set: PublicKeySet,
    sibling_key: Option<PublicKey>,
    section_chain: SectionChain,
    elders: BTreeSet<XorName>,
    dynamics: ElderDynamics,
}

#[derive(Clone)]
pub struct ElderDynamics {
    adult_reader: AdultReader,
    interaction: NodeInteraction,
    node_signing: NodeSigning,
}

impl ElderDynamics {
    pub fn new(network: Network) -> Self {
        Self {
            adult_reader: AdultReader::new(network.clone()),
            interaction: NodeInteraction::new(network.clone()),
            node_signing: NodeSigning::new(network),
        }
    }
}

impl ElderState {
    /// A snapshot of current state
    /// https://github.com/rust-lang/rust-clippy/issues?q=is%3Aissue+is%3Aopen+eval_order_dependence
    #[allow(clippy::eval_order_dependence)]
    pub async fn new(
        node_id: Ed25519PublicKey,
        knowledge: ElderKnowledge,
        dynamics: ElderDynamics,
    ) -> Result<Self> {
        Ok(Self {
            node_id,
            node_name: PublicKey::Ed25519(node_id).into(),
            prefix: knowledge.prefix,
            section_key_set: knowledge.section_key_set,
            sibling_key: knowledge.sibling_key.map(PublicKey::Bls),
            key_index: knowledge.our_key_set_index,
            section_chain: knowledge.section_chain,
            elders: knowledge.elders,
            dynamics,
        })
    }

    /// Get new state out of the provided elder knowledge.
    pub fn from(&self, knowledge: ElderKnowledge) -> Result<Self> {
        // if let Some(block) = knowledge.new_block {
        //     let parent_key = self.section_chain.last_key();
        //     self.section_chain.insert(parent_key, block.key, block.signature)?;
        // }
        Ok(Self {
            node_name: self.node_name,
            node_id: self.node_id,
            prefix: knowledge.prefix,
            section_key_set: knowledge.section_key_set,
            sibling_key: knowledge.sibling_key.map(PublicKey::Bls),
            key_index: knowledge.our_key_set_index,
            section_chain: knowledge.section_chain,
            elders: knowledge.elders,
            dynamics: self.dynamics.clone(),
        })
    }

    ///
    pub async fn set_joins_allowed(&mut self, joins_allowed: bool) -> Result<()> {
        self.dynamics
            .interaction
            .set_joins_allowed(joins_allowed)
            .await
    }

    // ---------------------------------------------------
    // ----------------- DYNAMIC STATE -------------------
    // ---------------------------------------------------

    /// Dynamic state
    pub async fn adults(&self) -> Vec<XorName> {
        self.dynamics.adult_reader.our_adults().await
    }

    /// Dynamic state
    pub async fn adults_sorted_by_distance_to(&self, name: &XorName, count: usize) -> Vec<XorName> {
        self.dynamics
            .adult_reader
            .our_adults_sorted_by_distance_to(name, count)
            .await
    }

    // ---------------------------------------------------
    // ----------------- STATIC STATE --------------------
    // ---------------------------------------------------

    /// Static state
    pub fn prefix(&self) -> &Prefix {
        &self.prefix
    }

    /// Static state
    pub fn node_name(&self) -> XorName {
        self.node_name
    }

    /// Static state
    pub fn node_id(&self) -> Ed25519PublicKey {
        self.node_id
    }

    /// Static state
    pub fn key_index(&self) -> usize {
        self.key_index
    }

    /// Static state
    pub fn section_public_key(&self) -> PublicKey {
        PublicKey::Bls(self.public_key_set().public_key())
    }

    /// Static state
    pub fn sibling_key(&self) -> Option<PublicKey> {
        self.sibling_key
    }

    /// Static state
    pub fn public_key_set(&self) -> &PublicKeySet {
        &self.section_key_set
    }

    /// Static state
    pub fn public_key_share(&self) -> PublicKeyShare {
        self.section_key_set.public_key_share(self.key_index)
    }

    /// Static state
    pub fn section_chain(&self) -> &SectionChain {
        &self.section_chain
    }

    /// Static state
    pub fn elder_names(&self) -> &BTreeSet<XorName> {
        &self.elders
    }

    /// Creates a detached BLS signature share of `data` if the `self` holds a BLS keypair share.
    pub async fn sign_as_elder<T: Serialize>(&self, data: &T) -> Result<SignatureShare> {
        let share = self
            .dynamics
            .node_signing
            .sign_as_elder(data, &self.public_key_set().public_key())
            .await?;
        Ok(SignatureShare {
            share,
            index: self.key_index,
        })
    }

    /// "Sort of" static; this is calling into routing layer
    /// but the underlying keys will not change.
    pub async fn sign_as_node<T: Serialize>(&self, data: &T) -> Result<Signature> {
        self.dynamics.node_signing.sign_as_node(&data).await
    }
}

#[derive(Clone)]
pub struct AdultReader {
    network: Network,
}

impl AdultReader {
    /// Access to the current state of our adult constellation
    pub fn new(network: Network) -> Self {
        Self { network }
    }

    /// Dynamic state
    pub async fn our_adults(&self) -> Vec<XorName> {
        self.network.our_adults().await
    }

    /// Dynamic state
    pub async fn our_adults_sorted_by_distance_to(
        &self,
        name: &XorName,
        count: usize,
    ) -> Vec<XorName> {
        self.network
            .our_adults_sorted_by_distance_to(name, count)
            .await
    }
}

#[derive(Clone)]
pub struct NodeSigning {
    network: Network,
}

impl NodeSigning {
    ///
    pub fn new(network: Network) -> Self {
        Self { network }
    }

    // "Sort of" static; this is calling into routing layer
    // but the underlying keys will not change.
    pub async fn sign_as_node<T: Serialize>(&self, data: &T) -> Result<Signature> {
        self.network.sign_as_node(&data).await
    }

    //
    pub async fn sign_as_elder<T: Serialize>(
        &self,
        data: &T,
        public_key: &bls::PublicKey,
    ) -> Result<bls::SignatureShare> {
        self.network.sign_as_elder(data, public_key).await
    }
}

#[derive(Clone)]
pub struct NodeInteraction {
    network: Network,
}

impl NodeInteraction {
    ///
    pub fn new(network: Network) -> Self {
        Self { network }
    }

    ///
    pub async fn set_joins_allowed(&mut self, joins_allowed: bool) -> Result<()> {
        self.network.set_joins_allowed(joins_allowed).await
    }
}

/// Info about the node.
#[derive(Clone)]
pub struct NodeInfo {
    ///
    pub genesis: bool,
    ///
    pub root_dir: PathBuf,
    ///
    pub used_space: UsedSpace,
    /// The key used by the node to receive earned rewards.
    pub reward_key: PublicKey,
}

impl NodeInfo {
    ///
    pub fn path(&self) -> &Path {
        self.root_dir.as_path()
    }
}
