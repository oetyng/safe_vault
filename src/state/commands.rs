// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    section_funds::SectionFunds,
    transfers::{replica_signing::ReplicaSigningImpl, replicas::ReplicaInfo},
};
use sn_data_types::{
    ActorHistory, Blob, BlobAddress, CreditAgreementProof, NodeAge, PublicKey, RewardAccumulation,
    RewardProposal, SignedTransfer, Transfer, TransferAgreementProof,
};
use sn_messaging::{
    client::{BlobRead, BlobWrite, DataCmd, DataQuery, Message},
    EndUser, MessageId, SrcLocation,
};

use sn_routing::XorName;
use std::collections::BTreeMap;

#[allow(clippy::large_enum_variant)]
pub enum AdultStateCommand {
    GetChunkForReplication {
        address: BlobAddress,
        id: MessageId,
        section: XorName,
    },
    StoreChunkForReplication(Blob),
    WriteChunk {
        write: BlobWrite,
        msg_id: MessageId,
        origin: EndUser,
    },
    ReadChunk {
        read: BlobRead,
        msg_id: MessageId,
        origin: EndUser,
    },
    CheckStorage,
}

#[allow(clippy::large_enum_variant)]
pub enum ElderStateCommand {
    ReceiveWalletAccumulation {
        accumulation: RewardAccumulation,
        section_key: PublicKey,
    },
    ReceiveChurnProposal(RewardProposal),
    MergeUserWallets(BTreeMap<PublicKey, ActorHistory>),
    SetNodeRewardsWallets(BTreeMap<XorName, (NodeAge, PublicKey)>),
    SetSectionFunds(SectionFunds),
    RemoveNodeWallet(XorName),
    AddPayment(CreditAgreementProof),
    UpdateReplicaInfo(ReplicaInfo<ReplicaSigningImpl>),
    DecreaseFullNodeCount(XorName),
    IncreaseFullNodeCount(PublicKey),
    TriggerChunkReplication(XorName),
    FinishChunkReplication(Blob),
    ProcessPayment {
        msg: Message,
        origin: EndUser,
    },
    WriteDataCmd {
        cmd: DataCmd,
        id: MessageId,
        origin: EndUser,
    },
    ReadDataCmd {
        query: DataQuery,
        id: MessageId,
        origin: EndUser,
    },
    RegisterTransfer {
        proof: TransferAgreementProof,
        msg_id: MessageId,
    },
    GetStoreCost {
        bytes: u64,
        msg_id: MessageId,
        origin: SrcLocation,
    },
    GetTransfersBalance {
        at: PublicKey,
        msg_id: MessageId,
        origin: SrcLocation,
    },
    GetTransfersHistory {
        at: PublicKey,
        msg_id: MessageId,
        origin: SrcLocation,
    },
    CreditWithoutProof(Transfer),
    ValidateTransfer {
        signed_transfer: SignedTransfer,
        msg_id: MessageId,
        origin: SrcLocation,
    },
    ReceivePropagated {
        proof: CreditAgreementProof,
        msg_id: MessageId,
        origin: SrcLocation,
    },
    GetTransferReplicaEvents {
        msg_id: MessageId,
        origin: SrcLocation,
    },
}
