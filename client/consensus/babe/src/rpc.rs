// Copyright 2019-2020 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! rpc api for babe.

use err_derive::*;
use futures::prelude::*;
use jsonrpc_core::BoxFuture;
use jsonrpc_derive::rpc;
use schnorrkel::vrf::{VRFProof, VRFOutput};
use sp_consensus_babe::{AuthorityId, Epoch, AuthorityIndex, BabePreDigest};
use crate::{SharedEpochChanges, authorship, epoch_changes::descendent_query, Config};
use sc_keystore::KeyStorePtr;
use std::sync::Arc;
use sp_core::crypto::Pair;
use sp_runtime::traits::{Block as BlockT, Header as _};
use sp_consensus::{SelectChain, Error as ConsensusError};
use sp_blockchain::{HeaderBackend, HeaderMetadata, Error as BlockChainError};
use serde::Deserialize;


/// Provides rpc methods for interacting with Babe
#[rpc]
pub trait Babe {
	/// query slot authorship info
//	#[rpc(name = "babe_epochAuthorship")]
	fn epoch_authorship(&self) -> BoxFuture<Vec<SlotAuthorship>>;
}

/// RPC handler for Babe
/// provides `babe_epochAuthorship` method for querying slot authorship data.
struct BabeRPC<B: BlockT, C> {
	client: Arc<C>,
	epoch_data: SharedEpochChanges<B>,
	keystore: KeyStorePtr,
	babe_config: Config,
}

impl<B, C> BabeRPC<B, C>
	where
		B: BlockT,
		C: SelectChain<B> + HeaderBackend<B> + HeaderMetadata<B, Error = BlockChainError> + 'static
{
	fn epoch_data(&self, ) -> Result<Epoch, ConsensusError> {
		let parent = self.client.best_chain()?;
		self.epoch_data.lock().epoch_for_child_of(
			descendent_query(&*self.client),
			&parent.hash(),
			parent.number().clone(),
			// FIXME: what slot_number goes here?
			0,
			|slot| self.babe_config.genesis_epoch(slot)
		)
			.map_err(|e| ConsensusError::ChainLookup(format!("{:?}", e)))?
			.map(|e| e.into_inner())
			.ok_or(ConsensusError::InvalidAuthoritiesSet)
	}
}

impl<B, C> Babe for BabeRPC<B, C>
	where
		B: BlockT,
		C: SelectChain<B> + HeaderBackend<B> + HeaderMetadata<B, Error = BlockChainError> + 'static,
{
	fn epoch_authorship(&self) -> BoxFuture<Vec<SlotAuthorship>> {
		// FIXME: spawn this heavy work on a threadpool and use a oneshot?
		// epoch data will be used to calculate slot information as well as claims
		let epoch_data = self.epoch_data().map_err(Error::Consensus);
		let (babe_config, keystore) = (self.babe_config.clone(), self.keystore.clone());
		let future = async move {
			let epoch_data = epoch_data?;
			let (epoch_start, epoch_end) = (epoch_data.start_slot, epoch_data.end_slot());
			let mut slots = vec![];

			for slot_number in epoch_start..epoch_end {
				let slot = authorship::claim_slot(slot_number, &epoch_data, &babe_config, &keystore);
				if let Some((claim, key)) = slot {
					let claim = match claim {
						BabePreDigest::Primary { vrf_output, vrf_proof, threshold, .. } => {
							let threshold = threshold.expect("threshold is set in the call to claim_slot; qed");
							BabeClaim::Primary {
								threshold,
								output: vrf_output,
								proof: vrf_proof,
								key: key.public(),
							}
						},
						BabePreDigest::Secondary { authority_index, .. } => {
							BabeClaim::Secondary {
								authority_index
							}
						}
					};

					slots.push(SlotAuthorship {
						claim,
						slot_number,
					});
				}
			}

			Ok(slots)
		}.boxed();

		Box::new(future.compat())
	}
}

/// slot authorship information
#[derive(Debug, Deserialize)]
struct SlotAuthorship {
	/// slot number in the epoch
	slot_number: u64,
	/// claim data
	claim: BabeClaim,
}

/// Babe claim
#[derive(Debug, Deserialize)]
enum BabeClaim {
	/// a primary claim for a given slot
	Primary {
		/// epoch threshold
		threshold: u128,
		/// key that makes this claim
		key: AuthorityId,
		/// Vrf output
		output: VRFOutput,
		/// Vrf proof
		proof: VRFProof,
	},
	/// a secondary claim for a given slot
	Secondary {
		authority_index: AuthorityIndex,
	}
}

#[derive(Error, derive_more::Display, derive_more::From)]
enum Error {
	#[display(fmt = "Consensus Error: {}", _0)]
	Consensus(ConsensusError)
}

impl From<Error> for jsonrpc_core::Error {
	fn from(error: Error) -> Self {
		jsonrpc_core::Error {
			message: format!("{}", error).into(),
			code: jsonrpc_core::ErrorCode::ServerError(1234),
			data: None
		}
	}
}
