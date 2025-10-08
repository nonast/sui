// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{Context, Object};
use std::{collections::BTreeSet, sync::Arc};

use crate::{error::RpcError, scope::Scope, task::watermark::Watermarks};

use super::checkpoint::Checkpoint;

/// Identifies a GraphQL query component that is used to determine the range of checkpoints for which data is available (for data that can be tied to a particular checkpoint)
///
/// Both `type_` and `field` are required. The `filter` is optional and provides retention information for filtered queries.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AvailableRangeKey {
    /// The GraphQL type to check retention for
    pub(crate) type_: String,

    /// The specific field within the type to check retention for
    pub(crate) field: Option<String>,

    /// Optional filter to check retention for filtered queries
    pub(crate) filters: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct AvailableRange {
    pub scope: Scope,
    pub first: u64,
}

/// Checkpoint range for which data is available.
#[Object]
impl AvailableRange {
    /// Inclusive lower checkpoint for which data is available.
    async fn first(&self) -> Result<Option<Checkpoint>, RpcError> {
        Ok(Checkpoint::with_sequence_number(
            self.scope.clone(),
            Some(self.first),
        ))
    }

    /// Inclusive upper checkpoint for which data is available.
    async fn last(&self) -> Result<Option<Checkpoint>, RpcError> {
        Ok(Checkpoint::with_sequence_number(self.scope.clone(), None))
    }
}

impl AvailableRange {
    /// Get retention information for a specific query type and field
    pub(crate) fn new(
        ctx: &Context<'_>,
        scope: &Scope,
        retention_key: AvailableRangeKey,
    ) -> Result<Self, RpcError> {
        let watermarks: &Arc<Watermarks> = ctx.data()?;
        let mut pipelines = BTreeSet::new();
        collect_pipelines(
            &retention_key.type_,
            retention_key.field.as_deref(),
            BTreeSet::from_iter(retention_key.filters.unwrap_or_default()),
            &mut pipelines,
        );

        let first =
            pipelines
                .iter()
                .try_fold(0, |acc: u64, pipeline| -> Result<u64, RpcError> {
                    let watermark = watermarks.pipeline_lo_watermark(pipeline)?;
                    let checkpoint = watermark.checkpoint();
                    Ok(acc.max(checkpoint))
                })?;

        Ok(Self {
            scope: scope.clone(),
            first,
        })
    }
}

macro_rules! pipeline_match {
    (
        $arg:expr,
        $(($type_:literal, ($($field:literal)|+), $filters:ident) => $action:block)*
    ) => {
        #[cfg(test)]
        {
            fn is_in_registry(type_: &str, field: &str) -> bool {
                use crate::schema;
                let schema = schema().finish();
                let registry = schema.names();
                registry.contains(&type_.to_string()) && (registry.contains(&field.to_string()) || field.is_empty())
            }
            $(
                $(
                    assert!(is_in_registry($type_, $field));
                )*
            )*
        }
        match ($arg) {
            $(
                ($type_, Some($($field)|+), $filters) => $action
            )*
            (_, _, _) => (),
        }
    }
}

/// Maps GraphQL query components to watermark pipeline names.
///
/// Determines which watermark pipelines are relevant for a given GraphQL query.
/// The pipeline names are used to query watermark data to determine the
/// checkpoint sequence range (available range) for which data is available.
///
fn collect_pipelines(
    type_: &str,
    field: Option<&str>,
    filters: BTreeSet<String>,
    pipelines: &mut BTreeSet<String>,
) {
    pipeline_match! {
        (type_, field, filters),

        ("Address", ("asObject"), filters) => {
            collect_pipelines("IObject", Some("objectAt"), filters, pipelines);
        }
        ("Address", ("transactions"), filters) => {
            let mut filters = filters;
            filters.insert("affectedAddress".to_string());
            collect_pipelines("Query", Some("transactions"), filters, pipelines);
        }
        ("Address", ("balance" | "balances" | "multiGetBalances" | "objects"), filters) => {
            collect_pipelines("IAddressable", field, filters, pipelines);
        }
        // Address has `dynamicFields` to allow for fetching fields on wrapped objects. But we do not want
        // to add `dynamicFields` to `IAddressable`, because that would incorrectly require MovePackage
        // to offer it.
        ("Address", ("dynamicField" | "dynamicFields" | "dynamicObjectField" | "multiGetDynamicFields" | "multiGetDynamicObjectFields"), filters) => {
            collect_pipelines("IMoveObject", field, filters, pipelines);
        }

        ("Checkpoint", ("transactions"), filters) => {
            let mut filters = filters;
            filters.insert("atCheckpoint".to_string());
            collect_pipelines("Query", Some("transactions"), filters, pipelines);
        }

        ("CoinMetadata", ("balance" | "balances" | "multiGetBalances"), filters) => {
            collect_pipelines("IAddressable", field, filters, pipelines);
        }
        ("CoinMetadata", ("dynamicFields"), filters) => {
            collect_pipelines("IMoveObject", field, filters, pipelines);
        }
        ("CoinMetadata", ("objects" | "receivedTransactions"), filters) => {
            collect_pipelines("IObject", field, filters, pipelines);
        }
        ("CoinMetadata", ("objectAt" | "objectVersionsAfter" | "objectVersionsBefore"), filters) => {
            collect_pipelines("IObject", field, filters, pipelines);
        }
        ("CoinMetadata", ("supply"), _filters) => {
            pipelines.insert("consistent".to_string());
        }

        ("Epoch", ("checkpoints"), filters) => {
            collect_pipelines("Query", Some("checkpoints"), filters, pipelines);
        }
        ("Epoch", ("coinDenyList"), _filters) => {
            pipelines.insert("obj_versions".to_string());
        }

        ("Event", ("contents" | "eventBcs" | "sender" | "sequenceNumber" | "timestamp" | "transaction" | "transactionModule"), filters) => {
            collect_pipelines("Query", Some("events"), filters, pipelines);
        }

        ("IAddressable", ("balance" | "balances" | "multiGetBalances" | "objects"), _filters) => {
            pipelines.insert("consistent".to_string());
        }
        ("IAddressable", ("defaultSuinsName"), _filters) => {
            pipelines.insert("obj_versions".to_string());
        }

        ("IMoveObject", ("dynamicFields"), _filters) => {
            pipelines.insert("consistent".to_string());
        }
        ("IMoveObject", ("contents" | "dynamicField" | "hasPublicTransfer" | "dynamicObjectField" | "objectVersionsBefore" | "moveObjectBcs" | "multiGetDynamicFields" | "multiGetDynamicObjectFields"), _filters) => {
            pipelines.insert("obj_versions".to_string());
        }

        ("IObject", ("objects"), _filters) => {
            pipelines.insert("consistent".to_string());
        }
        ("IObject", ("receivedTransactions"), filters) => {
            let mut filters = filters;
            filters.insert("affectedAddress".to_string());
            collect_pipelines("Query", Some("transactions"), filters, pipelines);
        }
        ("IObject", ("digest" | "objectAt" | "objectBcs" | "objectVersionsAfter" | "objectVersionsBefore" | "owner" | "previousTransaction" | "storageRebate" | "version"), _filters) => {
            pipelines.insert("obj_versions".to_string());
        }

        ("Object", ("address" | "balance" | "balances" | "defaultSuinsName" | "multiGetBalances" | "objects"), filters) => {
            collect_pipelines("IAddressable", field, filters, pipelines);
        }
        ("Object", ("asMoveObject" | "asMovePackage" | "dynamicField" | "dynamicFields" | "dynamicObjectField" | "multiGetDynamicFields" | "multiGetDynamicObjectFields"), filters) => {
            collect_pipelines("IMoveObject", field, filters, pipelines);
        }
        ("Object", ("digest" | "objectAt" | "objectBcs" | "objectVersionsAfter" | "objectVersionsBefore" | "owner" | "previousTransaction" | "storageRebate" | "version"), filters) => {
            collect_pipelines("IObject", field, filters, pipelines);
        }

        ("MovePackage", ("balance" | "balances" | "multiGetBalances"), filters) => {
            collect_pipelines("IAddressable", field, filters, pipelines);
        }
        ("MovePackage", ("digest" | "objectAt" | "objectBcs" | "objectVersionsAfter" | "objectVersionsBefore" | "owner" | "receivedTransactions" | "storageRebate" | "version"), filters) => {
            collect_pipelines("IObject", field, filters, pipelines);
        }

        ("Query", ("checkpoints"), _filters) => {
            pipelines.insert("cp_sequence_numbers".to_string());
        }
        ("Query", ("coinMetadata"), _filters) => {
            pipelines.insert("consistent".to_string());
        }
        ("Query", ("events"), filters) => {
            pipelines.insert("tx_digests".to_string());
            if filters.contains("module") {
                pipelines.insert("ev_emit_mod".to_string());
            } else {
                pipelines.insert("ev_struct_inst".to_string());
            }
        }
        ("Query", ("object"), _filters) => {
            pipelines.insert("obj_versions".to_string());
        }
        ("Query", ("objects"), _filters) => {
            pipelines.insert("consistent".to_string());
        }
        ("Query", ("objectVersions"), _filters) => {
            pipelines.insert("obj_versions".to_string());
        }
        ("Query", ("transactions"), filters) => {
            pipelines.insert("tx_digests".to_string());
            pipelines.insert("cp_sequence_numbers".to_string());

            if filters.contains("function") {
                pipelines.insert("tx_calls".to_string());
            } else if filters.contains("affectedAddress") {
                pipelines.insert("tx_affected_addresses".to_string());
            } else if filters.contains("kind") && !filters.contains("sentAddress") {
                pipelines.insert("tx_kinds".to_string());
            } else if filters.contains("affectedObjects") {
                pipelines.insert("tx_affected_objects".to_string());
            } else if filters.contains("sentAddress") {
                pipelines.insert("tx_affected_addresses".to_string());
            }
        }

        ("TransactionEffects", ("balanceChanges"), _filters) => {
            pipelines.insert("tx_balance_changes".to_string());
            pipelines.insert("tx_digests".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn test_collect_pipelines(
        type_: &str,
        field: Option<&str>,
        filters: BTreeSet<String>,
    ) -> BTreeSet<String> {
        let mut pipelines = BTreeSet::new();
        collect_pipelines(type_, field, filters, &mut pipelines);
        pipelines
    }

    #[test]
    fn test_address_as_object() {
        let result = test_collect_pipelines("Address", Some("asObject"), BTreeSet::new());
        assert!(result.contains("obj_versions"));
    }

    #[test]
    fn test_address_transactions() {
        let result = test_collect_pipelines("Address", Some("transactions"), BTreeSet::new());
        assert!(result.contains("tx_digests"));
        assert!(result.contains("tx_affected_addresses"));
    }

    #[test]
    fn test_address_consistent_fields() {
        let result = test_collect_pipelines("Address", Some("balance"), BTreeSet::new());
        assert!(result.contains("consistent"));
    }

    #[test]
    fn test_address_other_fields() {
        let result = test_collect_pipelines("Address", Some("defaultSuinsName"), BTreeSet::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_checkpoint_transactions() {
        let result = test_collect_pipelines("Checkpoint", Some("transactions"), BTreeSet::new());
        assert!(result.contains("cp_sequence_numbers"));
        assert!(result.contains("tx_digests"));
    }

    #[test]
    fn test_coin_metadata() {
        let result = test_collect_pipelines("CoinMetadata", Some("balance"), BTreeSet::new());
        assert!(result.contains("consistent"));
    }

    #[test]
    fn test_epoch_checkpoints() {
        let result = test_collect_pipelines("Epoch", Some("checkpoints"), BTreeSet::new());
        assert!(result.contains("cp_sequence_numbers"));
    }

    #[test]
    fn test_event() {
        let result = test_collect_pipelines("Event", Some("transaction"), BTreeSet::new());
        assert!(result.contains("ev_struct_inst"));
        assert!(result.contains("tx_digests"));
    }

    #[test]
    fn test_iobject_received_transactions() {
        let result =
            test_collect_pipelines("IObject", Some("receivedTransactions"), BTreeSet::new());
        assert!(result.contains("tx_digests"));
        assert!(result.contains("tx_affected_addresses"));
    }

    #[test]
    fn test_move_package() {
        let result = test_collect_pipelines("MovePackage", Some("balance"), BTreeSet::new());
        assert!(result.contains("consistent"));
    }

    #[test]
    fn test_query_address() {
        let result = test_collect_pipelines("Query", Some("address"), BTreeSet::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_query_checkpoints() {
        let result = test_collect_pipelines("Query", Some("checkpoints"), BTreeSet::new());
        assert!(result.contains("cp_sequence_numbers"));
    }

    #[test]
    fn test_query_coin_metadata() {
        let result = test_collect_pipelines("Query", Some("coinMetadata"), BTreeSet::new());
        assert!(result.contains("consistent"));
    }

    #[test]
    fn test_query_events_no_filters() {
        let result = test_collect_pipelines("Query", Some("events"), BTreeSet::new());
        assert!(result.contains("tx_digests"));
        assert!(result.contains("ev_struct_inst"));
    }

    #[test]
    fn test_query_events_with_module_and_sender_filter() {
        let result = test_collect_pipelines(
            "Query",
            Some("events"),
            BTreeSet::from_iter(vec!["module".to_string(), "sender".to_string()]),
        );
        assert!(result.contains("tx_digests"));
        assert!(result.contains("ev_emit_mod"));
    }

    #[test]
    fn test_query_events_with_sender_filter() {
        let result = test_collect_pipelines(
            "Query",
            Some("events"),
            BTreeSet::from_iter(vec!["sender".to_string()]),
        );
        assert!(result.contains("tx_digests"));
        assert!(result.contains("ev_struct_inst"));
    }

    #[test]
    fn test_query_objects() {
        let result = test_collect_pipelines("Query", Some("objects"), BTreeSet::new());
        assert!(result.contains("consistent"));
    }

    #[test]
    fn test_query_transactions_no_filters() {
        let result = test_collect_pipelines("Query", Some("transactions"), BTreeSet::new());
        assert!(result.contains("tx_digests"));
    }

    #[test]
    fn test_query_transactions_kind_filter() {
        let result = test_collect_pipelines(
            "Query",
            Some("transactions"),
            BTreeSet::from_iter(vec!["kind".to_string()]),
        );
        assert!(result.contains("tx_digests"));
        assert!(result.contains("tx_kinds"));
    }

    #[test]
    fn test_query_transactions_multiple_filters() {
        let result = test_collect_pipelines(
            "Query",
            Some("transactions"),
            BTreeSet::from_iter(vec![
                "kind".to_string(),
                "sentAddress".to_string(),
                "atCheckpoint".to_string(),
            ]),
        );
        assert!(result.contains("tx_digests"));
        assert!(result.contains("tx_affected_addresses"));
        assert!(result.contains("cp_sequence_numbers"));
    }

    #[test]
    fn test_transaction_effects_balance_changes() {
        let result = test_collect_pipelines(
            "TransactionEffects",
            Some("balanceChanges"),
            BTreeSet::new(),
        );
        assert!(result.contains("tx_balance_changes"));
        assert!(result.contains("tx_digests"));
    }

    #[test]
    fn test_catch_all() {
        let invalid: BTreeSet<String> =
            test_collect_pipelines("UnknownType", Some("field"), BTreeSet::new());
        assert!(invalid.is_empty());
        let valid: BTreeSet<String> =
            test_collect_pipelines("Address", Some("digests"), BTreeSet::new());
        assert!(valid.is_empty());
    }
}
