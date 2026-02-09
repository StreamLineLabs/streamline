//! Transaction handlers for the Kafka protocol.
//!
//! This module contains handlers for transaction management including
//! producer ID initialization, partition management, offset commits,
//! and transaction markers.


use crate::error::Result;
use crate::protocol::handlers::error_codes::*;
use kafka_protocol::messages::ProducerId as KafkaProducerId;
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::TransactionalId;
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, DescribeProducersRequest, DescribeProducersResponse, DescribeTransactionsRequest, DescribeTransactionsResponse, EndTxnRequest, EndTxnResponse, InitProducerIdRequest, InitProducerIdResponse, ListTransactionsRequest, ListTransactionsResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse, WriteTxnMarkersRequest, WriteTxnMarkersResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
use super::create_control_record_batch;
use super::{CONTROL_TYPE_COMMIT, CONTROL_TYPE_ABORT};
use tracing::{debug, info, warn};

impl KafkaHandler {
    /// Handle InitProducerId request
    pub(in crate::protocol) fn handle_init_producer_id(
        &self,
        request: InitProducerIdRequest,
    ) -> Result<InitProducerIdResponse> {
        let transactional_id = request.transactional_id.as_ref().map(|s| s.as_str());

        info!(
            transactional_id = ?transactional_id,
            transaction_timeout_ms = request.transaction_timeout_ms,
            "InitProducerId request"
        );

        // Initialize producer ID
        match self
            .producer_state_manager
            .init_producer_id(transactional_id, request.transaction_timeout_ms)
        {
            Ok((producer_id, producer_epoch)) => {
                info!(producer_id, producer_epoch, "Assigned producer ID");

                Ok(InitProducerIdResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(NONE)
                    .with_producer_id(KafkaProducerId(producer_id))
                    .with_producer_epoch(producer_epoch))
            }
            Err(e) => {
                warn!(error = %e, "Failed to initialize producer ID");
                // Return error response
                Ok(InitProducerIdResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(UNKNOWN_SERVER_ERROR)
                    .with_producer_id(KafkaProducerId(-1))
                    .with_producer_epoch(-1))
            }
        }
    }

    /// Handle AddPartitionsToTxn request
    pub(in crate::protocol) fn handle_add_partitions_to_txn(
        &self,
        request: AddPartitionsToTxnRequest,
    ) -> Result<AddPartitionsToTxnResponse> {
        use kafka_protocol::messages::add_partitions_to_txn_response::{
            AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
        };

        let transactional_id = request.v3_and_below_transactional_id.as_str();
        let producer_id = request.v3_and_below_producer_id.0;
        let producer_epoch = request.v3_and_below_producer_epoch;

        info!(
            transactional_id,
            producer_id, producer_epoch, "AddPartitionsToTxn request"
        );

        let coordinator = match &self.transaction_coordinator {
            Some(c) => c,
            None => {
                // No transaction coordinator - return error for all partitions
                let results: Vec<AddPartitionsToTxnTopicResult> = request
                    .v3_and_below_topics
                    .iter()
                    .map(|topic| {
                        let partition_results: Vec<AddPartitionsToTxnPartitionResult> = topic
                            .partitions
                            .iter()
                            .map(|p| {
                                AddPartitionsToTxnPartitionResult::default()
                                    .with_partition_index(*p)
                                    .with_partition_error_code(COORDINATOR_NOT_AVAILABLE)
                            })
                            .collect();
                        AddPartitionsToTxnTopicResult::default()
                            .with_name(topic.name.clone())
                            .with_results_by_partition(partition_results)
                    })
                    .collect();

                return Ok(AddPartitionsToTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_results_by_topic_v3_and_below(results));
            }
        };

        // Collect partitions from request
        let partitions: Vec<(String, i32)> = request
            .v3_and_below_topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|p| (topic.name.to_string(), *p))
            })
            .collect();

        // Call coordinator
        match coordinator.add_partitions_to_txn(
            transactional_id,
            producer_id,
            producer_epoch,
            partitions,
        ) {
            Ok(results) => {
                // Group results by topic
                let mut topic_results: std::collections::HashMap<
                    String,
                    Vec<AddPartitionsToTxnPartitionResult>,
                > = std::collections::HashMap::new();

                for ((topic, partition), error_code) in results {
                    topic_results.entry(topic).or_default().push(
                        AddPartitionsToTxnPartitionResult::default()
                            .with_partition_index(partition)
                            .with_partition_error_code(error_code),
                    );
                }

                let response_topics: Vec<AddPartitionsToTxnTopicResult> = topic_results
                    .into_iter()
                    .map(|(name, partitions)| {
                        AddPartitionsToTxnTopicResult::default()
                            .with_name(TopicName::from(StrBytes::from_string(name)))
                            .with_results_by_partition(partitions)
                    })
                    .collect();

                Ok(AddPartitionsToTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_results_by_topic_v3_and_below(response_topics))
            }
            Err(e) => {
                warn!(error = %e, "AddPartitionsToTxn failed");
                // Return error for all partitions
                let results: Vec<AddPartitionsToTxnTopicResult> = request
                    .v3_and_below_topics
                    .iter()
                    .map(|topic| {
                        let partition_results: Vec<AddPartitionsToTxnPartitionResult> = topic
                            .partitions
                            .iter()
                            .map(|p| {
                                AddPartitionsToTxnPartitionResult::default()
                                    .with_partition_index(*p)
                                    .with_partition_error_code(UNKNOWN_SERVER_ERROR)
                            })
                            .collect();
                        AddPartitionsToTxnTopicResult::default()
                            .with_name(topic.name.clone())
                            .with_results_by_partition(partition_results)
                    })
                    .collect();

                Ok(AddPartitionsToTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_results_by_topic_v3_and_below(results))
            }
        }
    }

    /// Handle AddOffsetsToTxn request
    pub(in crate::protocol) fn handle_add_offsets_to_txn(
        &self,
        request: AddOffsetsToTxnRequest,
    ) -> Result<AddOffsetsToTxnResponse> {
        let transactional_id = request.transactional_id.as_str();
        let producer_id = request.producer_id.0;
        let producer_epoch = request.producer_epoch;
        let group_id = request.group_id.as_str();

        info!(
            transactional_id,
            producer_id, producer_epoch, group_id, "AddOffsetsToTxn request"
        );

        let coordinator = match &self.transaction_coordinator {
            Some(c) => c,
            None => {
                return Ok(AddOffsetsToTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(COORDINATOR_NOT_AVAILABLE));
            }
        };

        match coordinator.add_offsets_to_txn(
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
        ) {
            Ok(()) => Ok(AddOffsetsToTxnResponse::default()
                .with_throttle_time_ms(0)
                .with_error_code(NONE)),
            Err(e) => {
                warn!(error = %e, "AddOffsetsToTxn failed");
                Ok(AddOffsetsToTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(UNKNOWN_SERVER_ERROR))
            }
        }
    }

    /// Handle EndTxn request
    pub(in crate::protocol) fn handle_end_txn(&self, request: EndTxnRequest) -> Result<EndTxnResponse> {
        let transactional_id = request.transactional_id.as_str();
        let producer_id = request.producer_id.0;
        let producer_epoch = request.producer_epoch;
        let committed = request.committed;

        info!(
            transactional_id,
            producer_id, producer_epoch, committed, "EndTxn request"
        );

        let coordinator = match &self.transaction_coordinator {
            Some(c) => c,
            None => {
                return Ok(EndTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(COORDINATOR_NOT_AVAILABLE));
            }
        };

        match coordinator.end_transaction(transactional_id, producer_id, producer_epoch, committed)
        {
            Ok(()) => {
                info!(
                    transactional_id,
                    committed, "Transaction ended successfully"
                );
                Ok(EndTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(NONE))
            }
            Err(e) => {
                warn!(error = %e, "EndTxn failed");
                Ok(EndTxnResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(UNKNOWN_SERVER_ERROR))
            }
        }
    }

    /// Handle TxnOffsetCommit request
    pub(in crate::protocol) fn handle_txn_offset_commit(
        &self,
        request: TxnOffsetCommitRequest,
    ) -> Result<TxnOffsetCommitResponse> {
        use kafka_protocol::messages::txn_offset_commit_response::{
            TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        };

        let transactional_id = request.transactional_id.as_str();
        let producer_id = request.producer_id.0;
        let producer_epoch = request.producer_epoch;
        let group_id = request.group_id.as_str();

        info!(
            transactional_id,
            producer_id, producer_epoch, group_id, "TxnOffsetCommit request"
        );

        let coordinator = match &self.transaction_coordinator {
            Some(c) => c,
            None => {
                // No transaction coordinator - return error for all partitions
                let results: Vec<TxnOffsetCommitResponseTopic> = request
                    .topics
                    .iter()
                    .map(|topic| {
                        let partition_results: Vec<TxnOffsetCommitResponsePartition> = topic
                            .partitions
                            .iter()
                            .map(|p| {
                                TxnOffsetCommitResponsePartition::default()
                                    .with_partition_index(p.partition_index)
                                    .with_error_code(COORDINATOR_NOT_AVAILABLE)
                            })
                            .collect();
                        TxnOffsetCommitResponseTopic::default()
                            .with_name(topic.name.clone())
                            .with_partitions(partition_results)
                    })
                    .collect();

                return Ok(TxnOffsetCommitResponse::default()
                    .with_throttle_time_ms(0)
                    .with_topics(results));
            }
        };

        // Collect offsets from request
        let offsets: Vec<(String, i32, i64, Option<String>)> = request
            .topics
            .iter()
            .flat_map(|topic| {
                topic.partitions.iter().map(|p| {
                    (
                        topic.name.to_string(),
                        p.partition_index,
                        p.committed_offset,
                        p.committed_metadata.as_ref().map(|m| m.to_string()),
                    )
                })
            })
            .collect();

        // Call coordinator
        match coordinator.txn_offset_commit(
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
            offsets,
        ) {
            Ok(results) => {
                // Group results by topic
                let mut topic_results: std::collections::HashMap<
                    String,
                    Vec<TxnOffsetCommitResponsePartition>,
                > = std::collections::HashMap::new();

                for ((topic, partition), error_code) in results {
                    topic_results.entry(topic).or_default().push(
                        TxnOffsetCommitResponsePartition::default()
                            .with_partition_index(partition)
                            .with_error_code(error_code),
                    );
                }

                let response_topics: Vec<TxnOffsetCommitResponseTopic> = topic_results
                    .into_iter()
                    .map(|(name, partitions)| {
                        TxnOffsetCommitResponseTopic::default()
                            .with_name(TopicName::from(StrBytes::from_string(name)))
                            .with_partitions(partitions)
                    })
                    .collect();

                Ok(TxnOffsetCommitResponse::default()
                    .with_throttle_time_ms(0)
                    .with_topics(response_topics))
            }
            Err(e) => {
                warn!(error = %e, "TxnOffsetCommit failed");
                // Return error for all partitions
                let results: Vec<TxnOffsetCommitResponseTopic> = request
                    .topics
                    .iter()
                    .map(|topic| {
                        let partition_results: Vec<TxnOffsetCommitResponsePartition> = topic
                            .partitions
                            .iter()
                            .map(|p| {
                                TxnOffsetCommitResponsePartition::default()
                                    .with_partition_index(p.partition_index)
                                    .with_error_code(UNKNOWN_SERVER_ERROR)
                            })
                            .collect();
                        TxnOffsetCommitResponseTopic::default()
                            .with_name(topic.name.clone())
                            .with_partitions(partition_results)
                    })
                    .collect();

                Ok(TxnOffsetCommitResponse::default()
                    .with_throttle_time_ms(0)
                    .with_topics(results))
            }
        }
    }

    // ========================================================================
    // New API Handlers - Transaction, Introspection, and Admin APIs
    // ========================================================================

    /// Handle WriteTxnMarkers request (API Key 27)
    /// Writes transaction markers (commit/abort) to partitions
    pub(in crate::protocol) fn handle_write_txn_markers(
        &self,
        request: WriteTxnMarkersRequest,
    ) -> Result<WriteTxnMarkersResponse> {
        use kafka_protocol::messages::write_txn_markers_response::{
            WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
        };

        info!(
            markers_count = request.markers.len(),
            "WriteTxnMarkers request"
        );

        let _coordinator = match &self.transaction_coordinator {
            Some(c) => c,
            None => {
                // No transaction coordinator - return error for all markers
                let results: Vec<WritableTxnMarkerResult> = request
                    .markers
                    .iter()
                    .map(|marker| {
                        let topic_results: Vec<WritableTxnMarkerTopicResult> = marker
                            .topics
                            .iter()
                            .map(|topic| {
                                let partition_results: Vec<WritableTxnMarkerPartitionResult> =
                                    topic
                                        .partition_indexes
                                        .iter()
                                        .map(|&partition_index| {
                                            WritableTxnMarkerPartitionResult::default()
                                                .with_partition_index(partition_index)
                                                .with_error_code(COORDINATOR_NOT_AVAILABLE)
                                        })
                                        .collect();
                                WritableTxnMarkerTopicResult::default()
                                    .with_name(topic.name.clone())
                                    .with_partitions(partition_results)
                            })
                            .collect();
                        WritableTxnMarkerResult::default()
                            .with_producer_id(marker.producer_id)
                            .with_topics(topic_results)
                    })
                    .collect();

                return Ok(WriteTxnMarkersResponse::default().with_markers(results));
            }
        };

        let mut results = Vec::new();

        for marker in &request.markers {
            let producer_id = marker.producer_id.0;
            let producer_epoch = marker.producer_epoch;
            let committed = marker.transaction_result;

            let control_type = if committed {
                CONTROL_TYPE_COMMIT
            } else {
                CONTROL_TYPE_ABORT
            };

            let mut topic_results = Vec::new();

            for topic in &marker.topics {
                let topic_name = topic.name.as_str();
                let mut partition_results = Vec::new();

                for &partition_index in &topic.partition_indexes {
                    // Get the high watermark for base offset
                    let error_code = match self
                        .topic_manager
                        .high_watermark(topic_name, partition_index)
                    {
                        Ok(base_offset) => {
                            let batch = create_control_record_batch(
                                base_offset,
                                producer_id,
                                producer_epoch,
                                control_type,
                            );

                            match self.topic_manager.append_batch(
                                topic_name,
                                partition_index,
                                batch,
                                1,
                            ) {
                                Ok(_) => {
                                    debug!(
                                        topic = topic_name,
                                        partition = partition_index,
                                        producer_id,
                                        committed,
                                        "Transaction marker written"
                                    );
                                    NONE
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to write transaction marker");
                                    UNKNOWN_SERVER_ERROR
                                }
                            }
                        }
                        Err(_) => UNKNOWN_TOPIC_OR_PARTITION,
                    };

                    partition_results.push(
                        WritableTxnMarkerPartitionResult::default()
                            .with_partition_index(partition_index)
                            .with_error_code(error_code),
                    );
                }

                topic_results.push(
                    WritableTxnMarkerTopicResult::default()
                        .with_name(topic.name.clone())
                        .with_partitions(partition_results),
                );
            }

            results.push(
                WritableTxnMarkerResult::default()
                    .with_producer_id(marker.producer_id)
                    .with_topics(topic_results),
            );
        }

        Ok(WriteTxnMarkersResponse::default().with_markers(results))
    }

    /// Handle DescribeProducers request (API Key 61)
    /// Returns information about active producers for partitions
    pub(in crate::protocol) fn handle_describe_producers(
        &self,
        request: DescribeProducersRequest,
    ) -> Result<DescribeProducersResponse> {
        use kafka_protocol::messages::describe_producers_response::{
            PartitionResponse, ProducerState, TopicResponse,
        };

        info!("DescribeProducers request");

        let mut topic_responses = Vec::new();

        for topic in &request.topics {
            let topic_name = topic.name.as_str();
            let mut partition_responses = Vec::new();

            for &partition_index in &topic.partition_indexes {
                let topic_exists = self.topic_manager.get_topic_metadata(topic_name).is_ok();
                let (error_code, producers) = if topic_exists {
                    // Get producer states from transaction coordinator if available
                    let producer_states: Vec<ProducerState> =
                        if let Some(ref txn_coordinator) = self.transaction_coordinator {
                            txn_coordinator
                                .get_ongoing_producer_ids(topic_name, partition_index)
                                .into_iter()
                                .map(|(producer_id, producer_epoch)| {
                                    ProducerState::default()
                                        .with_producer_id(KafkaProducerId(producer_id))
                                        .with_producer_epoch(producer_epoch as i32)
                                        .with_last_sequence(-1)
                                        .with_last_timestamp(-1)
                                        .with_coordinator_epoch(-1)
                                })
                                .collect()
                        } else {
                            vec![]
                        };
                    (NONE, producer_states)
                } else {
                    (UNKNOWN_TOPIC_OR_PARTITION, vec![])
                };

                partition_responses.push(
                    PartitionResponse::default()
                        .with_partition_index(partition_index)
                        .with_error_code(error_code)
                        .with_active_producers(producers),
                );
            }

            topic_responses.push(
                TopicResponse::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partition_responses),
            );
        }

        Ok(DescribeProducersResponse::default()
            .with_throttle_time_ms(0)
            .with_topics(topic_responses))
    }

    /// Handle DescribeTransactions request (API Key 65)
    /// Returns information about ongoing transactions
    pub(in crate::protocol) fn handle_describe_transactions(
        &self,
        request: DescribeTransactionsRequest,
    ) -> Result<DescribeTransactionsResponse> {
        use kafka_protocol::messages::describe_transactions_response::{
            TopicData, TransactionState,
        };

        info!(
            transactional_ids_count = request.transactional_ids.len(),
            "DescribeTransactions request"
        );

        let coordinator = match &self.transaction_coordinator {
            Some(c) => c,
            None => {
                // No transaction coordinator - return NOT_COORDINATOR for all
                let states: Vec<TransactionState> = request
                    .transactional_ids
                    .iter()
                    .map(|id| {
                        TransactionState::default()
                            .with_transactional_id(id.clone())
                            .with_error_code(COORDINATOR_NOT_AVAILABLE)
                    })
                    .collect();

                return Ok(DescribeTransactionsResponse::default()
                    .with_throttle_time_ms(0)
                    .with_transaction_states(states));
            }
        };

        let mut states = Vec::new();

        for transactional_id in &request.transactional_ids {
            let id_str = transactional_id.as_str();

            let state = match coordinator.get_transaction_full(id_str) {
                Some(txn) => {
                    // Convert partitions to TopicData format
                    let mut topic_map: std::collections::HashMap<String, Vec<i32>> =
                        std::collections::HashMap::new();
                    for partition in &txn.partitions {
                        topic_map
                            .entry(partition.topic.clone())
                            .or_default()
                            .push(partition.partition);
                    }

                    let topics: Vec<TopicData> = topic_map
                        .into_iter()
                        .map(|(topic, partitions)| {
                            TopicData::default()
                                .with_topic(TopicName::from(StrBytes::from_string(topic)))
                                .with_partitions(partitions)
                        })
                        .collect();

                    TransactionState::default()
                        .with_transactional_id(transactional_id.clone())
                        .with_error_code(NONE)
                        .with_transaction_state(StrBytes::from_string(txn.state.to_string()))
                        .with_transaction_timeout_ms(txn.timeout_ms as i32)
                        .with_producer_id(KafkaProducerId(txn.producer_id))
                        .with_producer_epoch(txn.producer_epoch)
                        .with_topics(topics)
                }
                None => TransactionState::default()
                    .with_transactional_id(transactional_id.clone())
                    .with_error_code(NONE)
                    .with_transaction_state(StrBytes::from_static_str("Empty")),
            };

            states.push(state);
        }

        Ok(DescribeTransactionsResponse::default()
            .with_throttle_time_ms(0)
            .with_transaction_states(states))
    }

    /// Handle ListTransactions request (API Key 66)
    /// Lists all transactions (optionally filtered)
    pub(in crate::protocol) fn handle_list_transactions(
        &self,
        request: ListTransactionsRequest,
    ) -> Result<ListTransactionsResponse> {
        use kafka_protocol::messages::list_transactions_response::TransactionState;

        info!("ListTransactions request");

        let coordinator = match &self.transaction_coordinator {
            Some(c) => c,
            None => {
                return Ok(ListTransactionsResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(COORDINATOR_NOT_AVAILABLE)
                    .with_transaction_states(vec![]));
            }
        };

        // Get all transactions with their full state
        let all_transactions = coordinator.list_transactions_with_state();

        // Apply filters
        let state_filter: Option<Vec<String>> = if request.state_filters.is_empty() {
            None
        } else {
            Some(
                request
                    .state_filters
                    .iter()
                    .map(|s| s.as_str().to_string())
                    .collect(),
            )
        };

        let producer_id_filter: Option<Vec<i64>> = if request.producer_id_filters.is_empty() {
            None
        } else {
            Some(request.producer_id_filters.iter().map(|p| p.0).collect())
        };

        let filtered: Vec<TransactionState> = all_transactions
            .into_iter()
            .filter(|(_, txn)| {
                // Filter by state if specified
                if let Some(ref states) = state_filter {
                    if !states.contains(&txn.state.to_string()) {
                        return false;
                    }
                }
                // Filter by producer ID if specified
                if let Some(ref ids) = producer_id_filter {
                    if !ids.contains(&txn.producer_id) {
                        return false;
                    }
                }
                true
            })
            .map(|(transactional_id, txn)| {
                TransactionState::default()
                    .with_transactional_id(TransactionalId::from(StrBytes::from_string(
                        transactional_id,
                    )))
                    .with_producer_id(KafkaProducerId(txn.producer_id))
                    .with_transaction_state(StrBytes::from_string(txn.state.to_string()))
            })
            .collect();

        Ok(ListTransactionsResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_transaction_states(filtered))
    }

}
