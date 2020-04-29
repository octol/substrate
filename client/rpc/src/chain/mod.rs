// Copyright 2017-2020 Parity Technologies (UK) Ltd.
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

//! Substrate blockchain API.

mod chain_full;
mod chain_light;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use futures::{future, StreamExt, TryStreamExt};
use log::warn;
use rpc::{
	Result as RpcResult,
	futures::{stream, Future, Sink, Stream},
};

use sc_rpc_api::Subscriptions;
use sc_client_api::{BlockchainEvents, light::{Fetcher, RemoteBlockchain}};
use jsonrpc_pubsub::{typed::Subscriber, SubscriptionId};
use sp_rpc::{number::NumberOrHex, list::ListOrValue};
use sp_runtime::{
	generic::{BlockId, SignedBlock},
	traits::{Block as BlockT, Header, NumberFor},
};

use self::error::{Result, Error, FutureResult};

pub use sc_rpc_api::chain::*;
use sp_blockchain::HeaderBackend;
use sc_client_api::BlockBackend;

/// Blockchain backend API
trait ChainBackend<Client, Block: BlockT>: Send + Sync + 'static
	where
		Block: BlockT + 'static,
		Client: HeaderBackend<Block> + BlockchainEvents<Block> + 'static,
{
	/// Get client reference.
	fn client(&self) -> &Arc<Client>;

	/// Get subscriptions reference.
	fn subscriptions(&self) -> &Subscriptions;

	/// Tries to unwrap passed block hash, or uses best block hash otherwise.
	fn unwrap_or_best(&self, hash: Option<Block::Hash>) -> Block::Hash {
		match hash.into() {
			None => self.client().info().best_hash,
			Some(hash) => hash,
		}
	}

	/// Get header of a relay chain block.
	fn header(&self, hash: Option<Block::Hash>) -> FutureResult<Option<Block::Header>>;

	/// Get header and body of a relay chain block.
	fn block(&self, hash: Option<Block::Hash>) -> FutureResult<Option<SignedBlock<Block>>>;

	/// Get hash of the n-th block in the canon chain.
	///
	/// By default returns latest block hash.
	fn block_hash(
		&self,
		number: Option<NumberOrHex<NumberFor<Block>>>,
	) -> Result<Option<Block::Hash>> {
		Ok(match number {
			None => Some(self.client().info().best_hash),
			Some(num_or_hex) => self.client()
				.header(BlockId::number(num_or_hex.to_number()?))
				.map_err(client_err)?
				.map(|h| h.hash()),
		})
	}

	/// Get hash of the last finalized block in the canon chain.
	fn finalized_head(&self) -> Result<Block::Hash> {
		Ok(self.client().info().finalized_hash)
	}

	/// All new head subscription
	fn subscribe_all_heads(
		&self,
		_metadata: crate::metadata::Metadata,
		subscriber: Subscriber<Block::Header>,
	) {
		subscribe_headers(
			self.client(),
			self.subscriptions(),
			subscriber,
			|| self.client().info().best_hash,
			|| self.client().import_notification_stream()
				.map(|notification| Ok::<_, ()>(notification.header))
				.compat(),
		)
	}

	/// Unsubscribe from all head subscription.
	fn unsubscribe_all_heads(
		&self,
		_metadata: Option<crate::metadata::Metadata>,
		id: SubscriptionId,
	) -> RpcResult<bool> {
		Ok(self.subscriptions().cancel(id))
	}

	/// New best head subscription
	fn subscribe_new_heads(
		&self,
		_metadata: crate::metadata::Metadata,
		subscriber: Subscriber<Block::Header>,
	) {
		subscribe_headers(
			self.client(),
			self.subscriptions(),
			subscriber,
			|| self.client().info().best_hash,
			|| self.client().import_notification_stream()
				.filter(|notification| future::ready(notification.is_new_best))
				.map(|notification| Ok::<_, ()>(notification.header))
				.compat(),
		)
	}

	/// Unsubscribe from new best head subscription.
	fn unsubscribe_new_heads(
		&self,
		_metadata: Option<crate::metadata::Metadata>,
		id: SubscriptionId,
	) -> RpcResult<bool> {
		Ok(self.subscriptions().cancel(id))
	}

	/// Finalized head subscription
	fn subscribe_finalized_heads(
		&self,
		_metadata: crate::metadata::Metadata,
		subscriber: Subscriber<Block::Header>,
	) {
		subscribe_headers(
			self.client(),
			self.subscriptions(),
			subscriber,
			|| self.client().info().finalized_hash,
			|| self.client().finality_notification_stream()
				.map(|notification| Ok::<_, ()>(notification.header))
				.compat(),
		)
	}

	/// Unsubscribe from finalized head subscription.
	fn unsubscribe_finalized_heads(
		&self,
		_metadata: Option<crate::metadata::Metadata>,
		id: SubscriptionId,
	) -> RpcResult<bool> {
		Ok(self.subscriptions().cancel(id))
	}

	/// Subscribe to finality events.
	///
	/// Informs subscribers about blocks with have beeen explicitly finalized
	/// by Grandpa and thus contain a justification.
	fn subscribe_finality(
		&self,
		metadata: crate::metadata::Metadata,
		subscriber: Subscriber<JustificationNotification<Block>>
	) {
		// TODO: Figure out how to get access to channel here
		let justification_stream = self.grandpa_subscription().justification_notification_stream();

		self.subscriptions().add(subscriber, |sink| {
			sink
				.sink_map_err(|e| warn!("Error sending notifications: {:?}", e))
				.send_all(stream)
				// we ignore the resulting Stream (if the first stream is over we are unsubscribed)
				.map(|_| ())
		});
	}

	/// Unsubscribe to finality events.
	fn unsubscribe_finality(
		&self,
		metadata: Option<crate::metadata::Metadata>,
		id: SubscriptionId,
	) -> RpcResult<bool> {
		Ok(self.Subscriptions.cancel(id))
	}
}

/// Create new state API that works on full node.
pub fn new_full<Block: BlockT, Client>(
	client: Arc<Client>,
	subscriptions: Subscriptions,
) -> Chain<Block, Client>
	where
		Block: BlockT + 'static,
		Client: BlockBackend<Block> + HeaderBackend<Block> + BlockchainEvents<Block> + 'static,
{
	Chain {
		backend: Box::new(self::chain_full::FullChain::new(client, subscriptions)),
	}
}

/// Create new state API that works on light node.
pub fn new_light<Block: BlockT, Client, F: Fetcher<Block>>(
	client: Arc<Client>,
	subscriptions: Subscriptions,
	remote_blockchain: Arc<dyn RemoteBlockchain<Block>>,
	fetcher: Arc<F>,
) -> Chain<Block, Client>
	where
		Block: BlockT + 'static,
		Client: BlockBackend<Block> + HeaderBackend<Block> + BlockchainEvents<Block> + 'static,
		F: Send + Sync + 'static,
{
	Chain {
		backend: Box::new(self::chain_light::LightChain::new(
			client,
			subscriptions,
			remote_blockchain,
			fetcher,
		)),
	}
}

/// Chain API with subscriptions support.
pub struct Chain<Block: BlockT, Client> {
	backend: Box<dyn ChainBackend<Client, Block>>,
}

impl<Block, Client> ChainApi<NumberFor<Block>, Block::Hash, Block::Header, SignedBlock<Block>> for
	Chain<Block, Client>
		where
			Block: BlockT + 'static,
			Client: HeaderBackend<Block> + BlockchainEvents<Block> + 'static,
{
	type Metadata = crate::metadata::Metadata;

	fn header(&self, hash: Option<Block::Hash>) -> FutureResult<Option<Block::Header>> {
		self.backend.header(hash)
	}

	fn block(&self, hash: Option<Block::Hash>) -> FutureResult<Option<SignedBlock<Block>>>
	{
		self.backend.block(hash)
	}

	fn block_hash(
		&self,
		number: Option<ListOrValue<NumberOrHex<NumberFor<Block>>>>
	) -> Result<ListOrValue<Option<Block::Hash>>> {
		match number {
			None => self.backend.block_hash(None).map(ListOrValue::Value),
			Some(ListOrValue::Value(number)) => self.backend.block_hash(Some(number)).map(ListOrValue::Value),
			Some(ListOrValue::List(list)) => Ok(ListOrValue::List(list
				.into_iter()
				.map(|number| self.backend.block_hash(Some(number)))
				.collect::<Result<_>>()?
			))
		}
	}

	fn finalized_head(&self) -> Result<Block::Hash> {
		self.backend.finalized_head()
	}

	fn subscribe_all_heads(&self, metadata: Self::Metadata, subscriber: Subscriber<Block::Header>) {
		self.backend.subscribe_all_heads(metadata, subscriber)
	}

	fn unsubscribe_all_heads(&self, metadata: Option<Self::Metadata>, id: SubscriptionId) -> RpcResult<bool> {
		self.backend.unsubscribe_all_heads(metadata, id)
	}

	fn subscribe_new_heads(&self, metadata: Self::Metadata, subscriber: Subscriber<Block::Header>) {
		self.backend.subscribe_new_heads(metadata, subscriber)
	}

	fn unsubscribe_new_heads(&self, metadata: Option<Self::Metadata>, id: SubscriptionId) -> RpcResult<bool> {
		self.backend.unsubscribe_new_heads(metadata, id)
	}

	fn subscribe_finalized_heads(&self, metadata: Self::Metadata, subscriber: Subscriber<Block::Header>) {
		self.backend.subscribe_finalized_heads(metadata, subscriber)
	}

	fn unsubscribe_finalized_heads(&self, metadata: Option<Self::Metadata>, id: SubscriptionId) -> RpcResult<bool> {
		self.backend.unsubscribe_finalized_heads(metadata, id)
	}

	// TODO: Figure out what `T` should be for `Subscriber`
	fn subscribe_finality(&self, metadata: Self::Metadata, subscriber: Subscriber<Block::Header>) {
		self.backend.subscribe_finality(metadata, subscriber)
	}

	fn unsubscribe_finality(&self, metadata: Option<Self::Metadata>, id: SubscriptionId) -> RpcResult<bool> {
		self.backend.unsubscribe_finality(metadata, id)
	}
}

/// Subscribe to new headers.
fn subscribe_headers<Block, Client, F, G, S, ERR>(
	client: &Arc<Client>,
	subscriptions: &Subscriptions,
	subscriber: Subscriber<Block::Header>,
	best_block_hash: G,
	stream: F,
) where
	Block: BlockT + 'static,
	Client: HeaderBackend<Block> + 'static,
	F: FnOnce() -> S,
	G: FnOnce() -> Block::Hash,
	ERR: ::std::fmt::Debug,
	S: Stream<Item=Block::Header, Error=ERR> + Send + 'static,
{
	subscriptions.add(subscriber, |sink| {
		// send current head right at the start.
		let header = client.header(BlockId::Hash(best_block_hash()))
			.map_err(client_err)
			.and_then(|header| {
				header.ok_or_else(|| "Best header missing.".to_owned().into())
			})
			.map_err(Into::into);

		// send further subscriptions
		let stream = stream()
			.map(|res| Ok(res))
			.map_err(|e| warn!("Block notification stream error: {:?}", e));

		sink
			.sink_map_err(|e| warn!("Error sending notifications: {:?}", e))
			.send_all(
				stream::iter_result(vec![Ok(header)])
					.chain(stream)
			)
			// we ignore the resulting Stream (if the first stream is over we are unsubscribed)
			.map(|_| ())
	});
}

fn client_err(err: sp_blockchain::Error) -> Error {
	Error::Client(Box::new(err))
}
