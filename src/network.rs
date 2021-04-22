// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    state_db::{get_network_keypair, store_network_keypair},
    utils, Config as NodeConfig, Error, Result,
};
use bls::PublicKeySet;
use bytes::Bytes;
use ed25519_dalek::PublicKey as Ed25519PublicKey;
use serde::Serialize;
use sn_data_types::{PublicKey, Signature, SignatureShare};
use sn_messaging::{client::ProcessMsg, Itinerary};
use sn_routing::{
    Config as RoutingConfig, Error as RoutingError, EventStream, Routing as RoutingNode,
    SectionChain,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::Path,
    sync::Arc,
};
use xor_name::{Prefix, XorName};

///
#[derive(Clone)]
pub struct Network {
    routing: Arc<RoutingNode>,
}

#[allow(missing_docs)]
impl Network {
    pub async fn new(root_dir: &Path, config: &NodeConfig) -> Result<(Self, EventStream)> {
        let keypair = get_network_keypair(root_dir).await?;

        let routing_config = RoutingConfig {
            first: config.is_first(),
            transport_config: config.network_config().clone(),
            keypair,
        };
        let (routing, event_stream) = RoutingNode::new(routing_config).await?;

        // Network keypair may have to be changed due to naming criteria or network requirements.
        store_network_keypair(root_dir, routing.keypair_as_bytes().await).await?;

        Ok((
            Self {
                routing: Arc::new(routing),
            },
            event_stream,
        ))
    }

    /// Sign with our node's ED25519 key
    #[allow(unused)]
    pub async fn sign_as_node<T: Serialize>(&self, data: &T) -> Result<Signature> {
        let data = utils::serialise(data)?;
        let sig = self.routing.sign_as_node(&data).await;
        Ok(Signature::Ed25519(sig))
    }

    /// Sign with our BLS PK Share
    pub async fn sign_as_elder<T: Serialize>(&self, data: &T) -> Result<SignatureShare> {
        let bls_pk = self
            .routing
            .public_key_set()
            .await
            .map_err(|_| Error::NoSectionPublicKey)?
            .public_key();
        let share = self
            .routing
            .sign_as_elder(&utils::serialise(data)?, &bls_pk)
            .await
            .map_err(Error::Routing)?;
        Ok(SignatureShare {
            share,
            index: self
                .routing
                .our_index()
                .await
                .map_err(|_| Error::NoSectionPublicKey)?,
        })
    }

    /// Sign with our BLS PK Share
    #[allow(unused)]
    pub async fn sign_as_elder_raw<T: Serialize>(&self, data: &T) -> Result<bls::SignatureShare> {
        let bls_pk = self
            .routing
            .public_key_set()
            .await
            .map_err(|_| Error::NoSectionPublicKey)?
            .public_key();
        let data = utils::serialise(data)?;
        let share = self
            .routing
            .sign_as_elder(&data, &bls_pk)
            .await
            .map_err(Error::Routing)?;
        Ok(share)
    }

    pub async fn age(&self) -> u8 {
        self.routing.age().await
    }

    pub async fn public_key(&self) -> Ed25519PublicKey {
        self.routing.public_key().await
    }

    pub async fn propose_offline(&self, name: XorName) -> Result<()> {
        self.routing
            .propose_offline(name)
            .await
            .map_err(|_| Error::NotAnElder)
    }

    pub async fn section_public_key(&self) -> Result<PublicKey> {
        Ok(PublicKey::Bls(
            self.routing
                .public_key_set()
                .await
                .map_err(|_| Error::NoSectionPublicKey)?
                .public_key(),
        ))
    }

    #[allow(unused)]
    pub async fn sibling_public_key(&self) -> Option<PublicKey> {
        let sibling_prefix = self.our_prefix().await.sibling();
        self.routing
            .section_key(&sibling_prefix)
            .await
            .map(PublicKey::Bls)
    }

    pub async fn matching_section(&self, name: &XorName) -> Option<bls::PublicKey> {
        let (key, _) = self.routing.matching_section(&name).await;
        key
    }

    pub async fn our_public_key_set(&self) -> Result<PublicKeySet> {
        self.routing.public_key_set().await.map_err(Error::Routing)
    }

    #[allow(unused)]
    pub async fn get_section_pk_by_name(&self, name: &XorName) -> Result<PublicKey> {
        let (pk, elders) = self.routing.matching_section(name).await;
        if let Some(pk) = pk {
            let pk = PublicKey::from(pk);
            Ok(pk)
        } else {
            Err(Error::NoSectionPublicKeyKnown(*name))
        }
    }

    pub async fn our_name(&self) -> XorName {
        self.routing.name().await
    }

    #[allow(unused)]
    pub async fn our_age(&self) -> u8 {
        self.routing.age().await
    }

    pub fn our_connection_info(&self) -> SocketAddr {
        self.routing.our_connection_info()
    }

    pub async fn our_prefix(&self) -> Prefix {
        self.routing.our_prefix().await
    }

    pub async fn section_chain(&self) -> SectionChain {
        self.routing.section_chain().await
    }

    #[allow(unused)]
    pub async fn matches_our_prefix(&self, name: XorName) -> bool {
        self.routing.matches_our_prefix(&XorName(name.0)).await
    }

    pub async fn send_message(
        &self,
        itinerary: Itinerary,
        content: Bytes,
    ) -> Result<(), RoutingError> {
        self.routing.send_message(itinerary, content, None).await
    }

    pub async fn set_joins_allowed(&mut self, joins_allowed: bool) -> Result<()> {
        self.routing
            .set_joins_allowed(joins_allowed)
            .await
            .map_err(Error::Routing)
    }

    /// Returns whether the node is Elder.
    pub async fn is_elder(&self) -> bool {
        self.routing.is_elder().await
    }

    /// get our PKshare
    pub async fn our_public_key_share(&self) -> Result<PublicKey> {
        let index = self.our_index().await?;
        Ok(PublicKey::from(
            self.our_public_key_set().await?.public_key_share(index),
        ))
    }

    /// BLS key index in routing for key shares
    pub async fn our_index(&self) -> Result<usize> {
        self.routing.our_index().await.map_err(Error::Routing)
    }

    // /// BLS key index in routing for key shares
    // pub async fn get_section_elders(&self, name: &XorName) -> Result<usize> {
    //     self.routing.get
    // }

    pub async fn our_elder_names(&self) -> BTreeSet<XorName> {
        self.routing
            .our_elders()
            .await
            .iter()
            .map(|p2p_node| XorName(p2p_node.name().0))
            .collect::<BTreeSet<_>>()
    }

    #[allow(unused)]
    pub async fn our_elder_addresses(&self) -> Vec<(XorName, SocketAddr)> {
        self.routing
            .our_elders()
            .await
            .iter()
            .map(|p2p_node| (XorName(p2p_node.name().0), *p2p_node.addr()))
            .collect::<Vec<_>>()
    }

    #[allow(unused)]
    pub async fn our_elder_addresses_sorted_by_distance_to(
        &self,
        name: &XorName,
    ) -> Vec<(XorName, SocketAddr)> {
        self.routing
            .our_elders_sorted_by_distance_to(&XorName(name.0))
            .await
            .into_iter()
            .map(|p2p_node| (XorName(p2p_node.name().0), *p2p_node.addr()))
            .collect::<Vec<_>>()
    }

    #[allow(unused)]
    pub async fn our_elder_names_sorted_by_distance_to(
        &self,
        name: &XorName,
        count: usize,
    ) -> Vec<XorName> {
        self.routing
            .our_elders_sorted_by_distance_to(&XorName(name.0))
            .await
            .into_iter()
            .take(count)
            .map(|p2p_node| XorName(p2p_node.name().0))
            .collect::<Vec<_>>()
    }

    pub async fn our_members(&self) -> BTreeMap<XorName, u8> {
        let elders: Vec<_> = self
            .routing
            .our_elders()
            .await
            .into_iter()
            .map(|peer| (*peer.name(), peer.age()))
            .collect();
        let adults: Vec<_> = self
            .routing
            .our_adults()
            .await
            .into_iter()
            .map(|peer| (*peer.name(), peer.age()))
            .collect();

        vec![elders, adults]
            .into_iter()
            .flatten()
            .collect::<BTreeMap<XorName, u8>>()
    }

    pub async fn our_adults(&self) -> Vec<XorName> {
        self.routing
            .our_adults()
            .await
            .into_iter()
            .map(|p2p_node| XorName(p2p_node.name().0))
            .collect::<Vec<_>>()
    }

    pub async fn our_adults_sorted_by_distance_to(
        &self,
        name: &XorName,
        count: usize,
    ) -> Vec<XorName> {
        self.routing
            .our_adults_sorted_by_distance_to(&XorName(name.0))
            .await
            .into_iter()
            .take(count)
            .map(|p2p_node| XorName(p2p_node.name().0))
            .collect::<Vec<_>>()
    }
}
