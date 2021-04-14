// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    chunks::Chunks, metadata::Metadata, section_funds::SectionFunds, transfers::Transfers, Error,
    Result,
};

pub(crate) struct AdultRole {
    // immutable chunks
    pub chunks: Chunks,
}

pub(crate) struct ElderRole {
    // data operations
    pub meta_data: Metadata,
    // transfers
    pub transfers: Transfers,
    // reward payouts
    pub section_funds: SectionFunds,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Role {
    Adult(AdultRole),
    Elder(ElderRole),
}

impl Role {
    // pub fn as_adult(&self) -> Result<&AdultRole> {
    //     match self {
    //         Self::Adult(adult_state) => Ok(adult_state),
    //         _ => Err(Error::NotAnAdult),
    //     }
    // }

    pub fn as_adult_mut(&mut self) -> Result<&mut AdultRole> {
        match self {
            Self::Adult(adult_state) => Ok(adult_state),
            _ => Err(Error::NotAnAdult),
        }
    }

    pub fn as_elder(&self) -> Result<&ElderRole> {
        match self {
            Self::Elder(elder_state) => Ok(elder_state),
            _ => Err(Error::NotAnElder),
        }
    }

    pub fn as_elder_mut(&mut self) -> Result<&mut ElderRole> {
        match self {
            Self::Elder(elder_state) => Ok(elder_state),
            _ => Err(Error::NotAnElder),
        }
    }
}
