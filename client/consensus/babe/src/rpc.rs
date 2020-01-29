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

use futures::{
	prelude::*,
	executor::ThreadPool,
	channel::oneshot,
	future,
};
use jsonrpc_core::BoxFuture;
use jsonrpc_derive::rpc;
use sp_consensus_babe::{AuthorityId, Epoch, AuthorityIndex, BabePreDigest};
use crate::{SharedEpochChanges, authorship, epoch_changes::descendent_query, Config};
use sc_keystore::KeyStorePtr;
use std::sync::Arc;
use sp_core::{crypto::Pair, Bytes};
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
	shared_epoch_changes: SharedEpochChanges<B>,
	keystore: KeyStorePtr,
	babe_config: Config,
	threadpool: ThreadPool,
}

impl<B: BlockT, C> BabeRPC<B, C> {
	pub fn new(
		client: Arc<C>,
		shared_epoch_changes: SharedEpochChanges<B>,
		keystore: KeyStorePtr,
		babe_config: Config
	) -> Self {
		let threadpool = ThreadPool::builder().pool_size(1).create().unwrap();
		Self {
			client,
			shared_epoch_changes,
			keystore,
			babe_config,
			threadpool,
		}
	}
}

impl<B, C> Babe for BabeRPC<B, C>
	where
		B: BlockT,
		C: SelectChain<B> + HeaderBackend<B> + HeaderMetadata<B, Error=BlockChainError> + 'static,
{
	fn epoch_authorship(&self) -> BoxFuture<Vec<SlotAuthorship>> {
		let (
			babe_config,
			keystore,
			shared_epoch,
			client,
		) = (
			self.babe_config.clone(),
			self.keystore.clone(),
			self.shared_epoch_changes.clone(),
			self.client.clone(),
		);

		// FIXME: get currrent slot_number from runtime.
		let epoch = epoch_data(&shared_epoch, &client, &babe_config, slot_number)
			.map_err(Error::Consensus);

		let (tx, rx) = oneshot::channel();

		let future = async move {
			let epoch = epoch?;
			let (epoch_start, epoch_end) = (epoch.start_slot, epoch.end_slot());
			let mut slots = vec![];

			for slot_number in epoch_start..=epoch_end {
				let epoch = epoch_data(&shared_epoch, &client, &babe_config, slot_number)
					.map_err(Error::Consensus)?;
				let slot = authorship::claim_slot(slot_number, &epoch, &babe_config, &keystore);
				if let Some((claim, key)) = slot {
					let claim = match claim {
						BabePreDigest::Primary { vrf_output, vrf_proof, threshold, .. } => {
							let threshold = threshold.expect("threshold is set in the call to claim_slot; qed");
							BabeClaim::Primary {
								threshold,
								output: Bytes(vrf_output.as_bytes().to_vec()),
								proof: Bytes(vrf_proof.to_bytes().to_vec()),
								key: key.public(),
							}
						}
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
		}.then(|result| {
			let _ = tx.send(result).expect("receiever is never dropped; qed");
			future::ready(())
		}).boxed();

		self.threadpool.spawn_ok(future);

		Box::new(async {
			rx.await.expect("sender is never dropped; qed")
		}.boxed().compat())
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
		output: Bytes,
		/// Vrf proof
		proof: Bytes,
	},
	/// a secondary claim for a given slot
	Secondary {
		authority_index: AuthorityIndex,
	},
}

#[derive(Debug, err_derive::Error)]
pub enum Error {
	#[error(display = "Consensus Error: {}", _0)]
	Consensus(ConsensusError),
}

impl From<Error> for jsonrpc_core::Error {
	fn from(error: Error) -> Self {
		jsonrpc_core::Error {
			message: format!("{}", error).into(),
			code: jsonrpc_core::ErrorCode::ServerError(1234),
			data: None,
		}
	}
}

fn epoch_data<B, C>(
	epoch_changes: &SharedEpochChanges<B>,
	client: &Arc<C>,
	babe_config: &Config,
	slot_number: u64,
) -> Result<Epoch, ConsensusError>
	where
		B: BlockT,
		C: SelectChain<B> + HeaderBackend<B> + HeaderMetadata<B, Error=BlockChainError> + 'static,
{
	let parent = client.best_chain()?;
	epoch_changes.lock().epoch_for_child_of(
		descendent_query(&**client),
		&parent.hash(),
		parent.number().clone(),
		slot_number,
		|slot| babe_config.genesis_epoch(slot),
	)
		.map_err(|e| ConsensusError::ChainLookup(format!("{:?}", e)))?
		.map(|e| e.into_inner())
		.ok_or(ConsensusError::InvalidAuthoritiesSet)
}