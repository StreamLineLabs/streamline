//! Data plane handlers for the Kafka protocol.
//!
//! This module contains handlers for produce, fetch, and offset operations
//! that form the core data path of the streaming platform.


use bytes::Bytes;
use crate::error::{Result, StreamlineError};
use crate::protocol::handlers::error_codes::*;
use crate::storage::topic::validate_topic_name;
#[cfg(feature = "clustering")]
use crate::replication::AcksPolicy;
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::{
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
use super::SessionManager;
use tracing::{debug, error, warn};

impl KafkaHandler {
    /// Handle Produce request
    pub(in crate::protocol) async fn handle_produce(
        &self,
        request: ProduceRequest,
        session_manager: &SessionManager,
        client_id: Option<&str>,
    ) -> Result<ProduceResponse> {
        use crate::storage::SequenceValidationResult;
        use kafka_protocol::messages::produce_response::{
            PartitionProduceResponse, TopicProduceResponse,
        };
        use kafka_protocol::records::{RecordBatchDecoder, NO_PRODUCER_ID, NO_SEQUENCE};

        // Error codes for idempotent producer
        const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
        const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
        const INVALID_PRODUCER_EPOCH: i16 = 47;

        // Parse acks policy from request (only used with clustering for write eligibility checks)
        #[cfg(feature = "clustering")]
        let acks_policy = AcksPolicy::from_kafka_acks(request.acks);
        #[cfg(feature = "clustering")]
        debug!(acks = %acks_policy, timeout_ms = request.timeout_ms, "Processing produce request");
        #[cfg(not(feature = "clustering"))]
        debug!(
            acks = request.acks,
            timeout_ms = request.timeout_ms,
            "Processing produce request"
        );

        let mut topic_responses = Vec::new();

        for topic_data in request.topic_data.iter() {
            let topic_name = topic_data.name.as_str().to_string();

            // Validate topic name before any operations
            if let Err(e) = validate_topic_name(&topic_name) {
                debug!(topic = %topic_name, error = %e, "Invalid topic name in produce request");
                let partition_responses: Vec<_> = topic_data
                    .partition_data
                    .iter()
                    .map(|pd| {
                        PartitionProduceResponse::default()
                            .with_index(pd.index)
                            .with_error_code(INVALID_TOPIC_EXCEPTION)
                            .with_base_offset(-1)
                            .with_log_append_time_ms(-1)
                            .with_log_start_offset(-1)
                    })
                    .collect();

                topic_responses.push(
                    TopicProduceResponse::default()
                        .with_name(topic_data.name.clone())
                        .with_partition_responses(partition_responses),
                );
                continue;
            }

            // Handle topic auto-creation based on configuration
            let topic_exists = if self.auto_create_topics {
                // Auto-create topic if needed (Kafka default behavior)
                let _ = self.topic_manager.get_or_create_topic(&topic_name, 1);
                true
            } else {
                // Check if topic exists without creating
                self.topic_manager.get_topic_metadata(&topic_name).is_ok()
            };

            // If topic doesn't exist and auto-creation is disabled, return error for all partitions
            if !topic_exists {
                let partition_responses: Vec<_> = topic_data
                    .partition_data
                    .iter()
                    .map(|pd| {
                        PartitionProduceResponse::default()
                            .with_index(pd.index)
                            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                            .with_base_offset(-1)
                            .with_log_append_time_ms(-1)
                            .with_log_start_offset(-1)
                    })
                    .collect();

                topic_responses.push(
                    TopicProduceResponse::default()
                        .with_name(topic_data.name.clone())
                        .with_partition_responses(partition_responses),
                );
                continue;
            }

            let mut partition_responses = Vec::new();

            for partition_data in topic_data.partition_data.iter() {
                let partition_index = partition_data.index;

                // In cluster mode, check if we can accept the write based on acks policy
                #[cfg(feature = "clustering")]
                let can_write = if let Some(ref replication_manager) = self.replication_manager {
                    replication_manager
                        .can_accept_write(&topic_name, partition_index, acks_policy)
                        .await
                } else {
                    // Single-node mode - always accept
                    true
                };
                // Without clustering, single-node mode always accepts writes
                #[cfg(not(feature = "clustering"))]
                let can_write = true;

                if !can_write {
                    // We're not the leader or don't have enough ISR
                    #[cfg(feature = "clustering")]
                    debug!(
                        topic = %topic_name,
                        partition = partition_index,
                        acks = %acks_policy,
                        "Cannot accept write - not leader or insufficient ISR"
                    );
                    let partition_response = PartitionProduceResponse::default()
                        .with_index(partition_index)
                        .with_error_code(NOT_LEADER_OR_FOLLOWER)
                        .with_base_offset(-1)
                        .with_log_append_time_ms(-1)
                        .with_log_start_offset(-1);
                    partition_responses.push(partition_response);
                    continue;
                }

                // Extract records from the batch
                let (base_offset, error_code) = if let Some(ref records) = partition_data.records {
                    // Use Bytes clone (cheap - reference counted) instead of copy_from_slice
                    // This avoids a full memory copy since Bytes uses Arc internally
                    let batch_bytes = records.clone();

                    // Type for the decompressor function
                    type DecompressFn = fn(
                        &mut Bytes,
                        kafka_protocol::records::Compression,
                    ) -> anyhow::Result<Bytes>;

                    // Extract producer info from the record batch
                    // We need both base_sequence and record_count to track the last sequence
                    let (producer_id, producer_epoch, base_sequence, record_count) =
                        match RecordBatchDecoder::decode_with_custom_compression::<
                            Bytes,
                            DecompressFn,
                        >(&mut batch_bytes.clone(), None)
                        {
                            Ok(decoded_records) => {
                                if let Some(first_record) = decoded_records.first() {
                                    (
                                        first_record.producer_id,
                                        first_record.producer_epoch,
                                        first_record.sequence,
                                        decoded_records.len() as i32,
                                    )
                                } else {
                                    (NO_PRODUCER_ID, -1, NO_SEQUENCE, 0)
                                }
                            }
                            Err(e) => {
                                // Decode failure indicates corrupted data - return error
                                warn!(
                                    topic = %topic_name,
                                    partition = partition_index,
                                    error = %e,
                                    "Failed to decode record batch - corrupted data"
                                );
                                let log_start_offset = self
                                    .topic_manager
                                    .earliest_offset(&topic_name, partition_index)
                                    .unwrap_or(0);
                                let partition_response = PartitionProduceResponse::default()
                                    .with_index(partition_index)
                                    .with_error_code(CORRUPT_MESSAGE)
                                    .with_base_offset(-1)
                                    .with_log_append_time_ms(-1)
                                    .with_log_start_offset(log_start_offset);
                                partition_responses.push(partition_response);
                                continue;
                            }
                        };

                    // Validate sequence for idempotent producers
                    if producer_id != NO_PRODUCER_ID && base_sequence != NO_SEQUENCE {
                        match self.producer_state_manager.validate_sequence(
                            &topic_name,
                            partition_index,
                            producer_id,
                            producer_epoch,
                            base_sequence,
                        ) {
                            Ok(SequenceValidationResult::Valid) => {
                                // Proceed with append
                            }
                            Ok(SequenceValidationResult::Duplicate(existing_offset)) => {
                                debug!(
                                    topic = %topic_name,
                                    partition = partition_index,
                                    producer_id,
                                    sequence = base_sequence,
                                    existing_offset,
                                    "Duplicate sequence detected, returning existing offset"
                                );
                                let log_start_offset = self
                                    .topic_manager
                                    .earliest_offset(&topic_name, partition_index)
                                    .unwrap_or(0);
                                let partition_response = PartitionProduceResponse::default()
                                    .with_index(partition_index)
                                    .with_error_code(DUPLICATE_SEQUENCE_NUMBER)
                                    .with_base_offset(existing_offset)
                                    .with_log_append_time_ms(chrono::Utc::now().timestamp_millis())
                                    .with_log_start_offset(log_start_offset);
                                partition_responses.push(partition_response);
                                continue;
                            }
                            Ok(SequenceValidationResult::OutOfOrder { expected, received }) => {
                                warn!(
                                    topic = %topic_name,
                                    partition = partition_index,
                                    producer_id,
                                    expected,
                                    received,
                                    "Out of order sequence"
                                );
                                let log_start_offset = self
                                    .topic_manager
                                    .earliest_offset(&topic_name, partition_index)
                                    .unwrap_or(0);
                                let partition_response = PartitionProduceResponse::default()
                                    .with_index(partition_index)
                                    .with_error_code(OUT_OF_ORDER_SEQUENCE_NUMBER)
                                    .with_base_offset(-1)
                                    .with_log_append_time_ms(-1)
                                    .with_log_start_offset(log_start_offset);
                                partition_responses.push(partition_response);
                                continue;
                            }
                            Ok(SequenceValidationResult::InvalidEpoch {
                                current_epoch,
                                received_epoch,
                            }) => {
                                warn!(
                                    topic = %topic_name,
                                    partition = partition_index,
                                    producer_id,
                                    current_epoch,
                                    received_epoch,
                                    "Invalid producer epoch"
                                );
                                let log_start_offset = self
                                    .topic_manager
                                    .earliest_offset(&topic_name, partition_index)
                                    .unwrap_or(0);
                                let partition_response = PartitionProduceResponse::default()
                                    .with_index(partition_index)
                                    .with_error_code(INVALID_PRODUCER_EPOCH)
                                    .with_base_offset(-1)
                                    .with_log_append_time_ms(-1)
                                    .with_log_start_offset(log_start_offset);
                                partition_responses.push(partition_response);
                                continue;
                            }
                            Err(e) => {
                                error!(
                                    topic = %topic_name,
                                    partition = partition_index,
                                    error = %e,
                                    "Failed to validate sequence"
                                );
                                // Continue with append anyway for non-critical errors
                            }
                        }
                    }

                    // ── Schema Validation (if enabled) ───────────────────────
                    #[cfg(feature = "schema-registry")]
                    if let Some(ref validator) = self.schema_validator {
                        if let Err(e) = validator
                            .validate_produce(&topic_name, None, &batch_bytes)
                            .await
                        {
                            debug!(
                                topic = %topic_name,
                                partition = partition_index,
                                error = %e,
                                "Schema validation failed on produce"
                            );
                            // Return INVALID_RECORD error for this partition
                            let partition_response = PartitionProduceResponse::default()
                                .with_index(partition_index)
                                .with_error_code(87) // INVALID_RECORD
                                .with_base_offset(-1)
                                .with_log_append_time_ms(-1)
                                .with_log_start_offset(0);
                            partition_responses.push(partition_response);
                            continue;
                        }
                    }

                    // Append the Kafka record batch using append_batch
                    // This method:
                    // 1. Atomically reserves `record_count` offsets
                    // 2. Updates the batch header's baseOffset to the broker-assigned offset
                    // 3. Stores the batch with correct offset tracking
                    // Reuse batch_bytes (already cloned from records) - avoids third copy
                    let effective_record_count = if record_count > 0 { record_count } else { 1 };

                    // Clone batch bytes for post-append embedding (if AI feature enabled)
                    #[cfg(feature = "ai")]
                    let batch_bytes_for_embed = batch_bytes.clone();

                    // Route append to owning shard for thread-per-core locality
                    // This achieves cache affinity: partition data is always processed by the same CPU
                    let append_result = if let Some(ref rt) = self.sharded_runtime {
                        let topic_manager = self.topic_manager.clone();
                        let topic_name_owned = topic_name.clone();
                        match rt.submit_for_partition_with_result(
                            partition_index as u32,
                            move || {
                                topic_manager.append_batch(
                                    &topic_name_owned,
                                    partition_index,
                                    batch_bytes,
                                    effective_record_count,
                                )
                            },
                        ) {
                            Ok(receiver) => receiver.await.unwrap_or_else(|_| {
                                Err(StreamlineError::server("shard_append", "task cancelled"))
                            }),
                            Err(e) => {
                                error!(error = %e, "Failed to submit append to shard");
                                Err(StreamlineError::server("shard_routing", e.to_string()))
                            }
                        }
                    } else {
                        self.topic_manager.append_batch(
                            &topic_name,
                            partition_index,
                            batch_bytes,
                            effective_record_count,
                        )
                    };

                    match append_result {
                        Ok(base_offset) => {
                            // Record the produce for sequence tracking (idempotent producers)
                            // Pass both base_sequence and record_count so the state manager can:
                            // 1. Store base_sequence in window for duplicate detection
                            // 2. Update last_sequence to base + count - 1 for next expected calculation
                            if producer_id != NO_PRODUCER_ID && base_sequence != NO_SEQUENCE {
                                if let Err(e) = self.producer_state_manager.record_produce(
                                    &topic_name,
                                    partition_index,
                                    producer_id,
                                    producer_epoch,
                                    base_sequence,
                                    effective_record_count,
                                    base_offset,
                                ) {
                                    warn!(
                                        topic = %topic_name,
                                        partition = partition_index,
                                        producer_id,
                                        error = %e,
                                        "Failed to record produce for sequence tracking"
                                    );
                                }
                            }

                            // Update replication manager's LEO (only in cluster mode)
                            #[cfg(feature = "clustering")]
                            if let Some(ref replication_manager) = self.replication_manager {
                                // The new LEO is base_offset + record_count (next offset to write)
                                // Use saturating_add to prevent integer overflow (unreachable in practice
                                // since i64::MAX is 9.2 quintillion, but prevents undefined behavior)
                                let new_leo =
                                    base_offset.saturating_add(effective_record_count as i64);
                                replication_manager
                                    .set_log_end_offset(&topic_name, partition_index, new_leo)
                                    .await;
                            }
                            // ── AI Auto-Embedding (if enabled) ───────────────
                            #[cfg(feature = "ai")]
                            if let Some(ref embed) = self.auto_embed_interceptor {
                                let ts = chrono::Utc::now().timestamp_millis();
                                // Fire-and-forget: embedding is async, don't block produce
                                let embed = embed.clone();
                                let topic = topic_name.clone();
                                let data = batch_bytes_for_embed.clone();
                                tokio::spawn(async move {
                                    let _ = embed
                                        .on_produce(&topic, partition_index, base_offset, &data, ts)
                                        .await;
                                });
                            }
                            (base_offset, NONE)
                        }
                        Err(e) => {
                            error!(
                                topic = %topic_name,
                                partition = partition_index,
                                error = %e,
                                "Failed to append batch"
                            );
                            (-1, OFFSET_OUT_OF_RANGE)
                        }
                    }
                } else {
                    (-1, OFFSET_OUT_OF_RANGE)
                };

                // For acks=all, we would ideally wait for ISR acknowledgment here
                // For now, we handle it synchronously (HWM advancement happens via replication)
                // A full implementation would:
                // 1. Wait for timeout_ms or ISR acknowledgment (whichever comes first)
                // 2. Return error if timeout occurs before ISR ack
                //
                // Since we're in single-node mode or leader-only ack, proceed immediately
                #[cfg(feature = "clustering")]
                if base_offset >= 0 && acks_policy.requires_all_isr() {
                    if let Some(ref replication_manager) = self.replication_manager {
                        // Try to advance HWM (in single-node mode, this should immediately succeed)
                        let _ = replication_manager
                            .try_advance_hwm(&topic_name, partition_index)
                            .await;
                    }
                }

                let log_start_offset = self
                    .topic_manager
                    .earliest_offset(&topic_name, partition_index)
                    .unwrap_or(0);

                let partition_response = PartitionProduceResponse::default()
                    .with_index(partition_index)
                    .with_error_code(error_code)
                    .with_base_offset(base_offset)
                    .with_log_append_time_ms(chrono::Utc::now().timestamp_millis())
                    .with_log_start_offset(log_start_offset);

                partition_responses.push(partition_response);
            }

            let topic_response = TopicProduceResponse::default()
                .with_name(TopicName::from(StrBytes::from_string(topic_name)))
                .with_partition_responses(partition_responses);

            topic_responses.push(topic_response);
        }

        // Calculate throttle time if quota manager is enabled
        let throttle_time_ms = if let Some(ref quota_manager) = self.quota_manager {
            // Calculate total bytes in the produce request
            let total_bytes: u64 = request
                .topic_data
                .iter()
                .flat_map(|t| t.partition_data.iter())
                .filter_map(|p| p.records.as_ref())
                .map(|r| r.len() as u64)
                .sum();

            // Use actual client info for quota tracking
            let peer_ip = session_manager
                .peer_addr()
                .map(|addr| addr.ip())
                .unwrap_or_else(|| std::net::Ipv4Addr::LOCALHOST.into());
            quota_manager.check_producer_quota(client_id, peer_ip, total_bytes)
        } else {
            0
        };

        let response = ProduceResponse::default()
            .with_responses(topic_responses)
            .with_throttle_time_ms(throttle_time_ms);

        Ok(response)
    }

    /// Handle Fetch request
    pub(in crate::protocol) async fn handle_fetch(
        &self,
        request: FetchRequest,
        session_manager: &SessionManager,
        client_id: Option<&str>,
    ) -> Result<FetchResponse> {
        use kafka_protocol::messages::fetch_response::{
            AbortedTransaction, FetchableTopicResponse, PartitionData,
        };

        // Isolation level: 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED
        let isolation_level = request.isolation_level;
        let is_read_committed = isolation_level == 1;

        let mut topic_responses = Vec::new();

        for topic in request.topics.iter() {
            let topic_name = topic.topic.as_str().to_string();
            let mut partition_responses = Vec::new();

            for partition in topic.partitions.iter() {
                let partition_index = partition.partition;
                let fetch_offset = partition.fetch_offset;
                let max_bytes = partition.partition_max_bytes;

                debug!(
                    topic = %topic_name,
                    partition = partition_index,
                    fetch_offset = fetch_offset,
                    max_bytes = max_bytes,
                    isolation_level = isolation_level,
                    "Fetch request for partition"
                );

                // Route read to owning shard for thread-per-core locality
                // This achieves cache affinity: partition data is always read by the same CPU
                let max_records = (max_bytes / 1024).max(1) as usize;

                // For READ_COMMITTED, compute LSO before reading so we can limit the read range
                let lso_for_read = if is_read_committed {
                    if let Some(ref txn_coordinator) = self.transaction_coordinator {
                        let hw = self
                            .topic_manager
                            .high_watermark(&topic_name, partition_index)
                            .unwrap_or(0);
                        match txn_coordinator
                            .get_first_unstable_offset(&topic_name, partition_index)
                        {
                            Some(unstable_offset) => Some(unstable_offset.min(hw)),
                            None => Some(hw),
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let read_result = if let Some(lso) = lso_for_read {
                    // READ_COMMITTED: read up to LSO only
                    if let Some(ref rt) = self.sharded_runtime {
                        let topic_manager = self.topic_manager.clone();
                        let topic_name_owned = topic_name.clone();
                        match rt.submit_for_partition_with_result(
                            partition_index as u32,
                            move || {
                                topic_manager.read_committed_with_lso(
                                    &topic_name_owned,
                                    partition_index,
                                    fetch_offset,
                                    max_records,
                                    lso,
                                )
                            },
                        ) {
                            Ok(receiver) => receiver.await.unwrap_or_else(|_| {
                                Err(StreamlineError::server("shard_fetch", "task cancelled"))
                            }),
                            Err(e) => {
                                error!(error = %e, "Failed to submit fetch to shard");
                                Err(StreamlineError::server("shard_routing", e.to_string()))
                            }
                        }
                    } else {
                        self.topic_manager.read_committed_with_lso(
                            &topic_name,
                            partition_index,
                            fetch_offset,
                            max_records,
                            lso,
                        )
                    }
                } else {
                    // READ_UNCOMMITTED: read all available records
                    if let Some(ref rt) = self.sharded_runtime {
                        let topic_manager = self.topic_manager.clone();
                        let topic_name_owned = topic_name.clone();
                        match rt.submit_for_partition_with_result(
                            partition_index as u32,
                            move || {
                                topic_manager.read_with_ttl(
                                    &topic_name_owned,
                                    partition_index,
                                    fetch_offset,
                                    max_records,
                                )
                            },
                        ) {
                            Ok(receiver) => receiver.await.unwrap_or_else(|_| {
                                Err(StreamlineError::server("shard_fetch", "task cancelled"))
                            }),
                            Err(e) => {
                                error!(error = %e, "Failed to submit fetch to shard");
                                Err(StreamlineError::server("shard_routing", e.to_string()))
                            }
                        }
                    } else {
                        self.topic_manager.read_with_ttl(
                            &topic_name,
                            partition_index,
                            fetch_offset,
                            max_records,
                        )
                    }
                };

                let (records, high_watermark, error_code) = match read_result {
                    Ok(records) => {
                        let hw = self
                            .topic_manager
                            .high_watermark(&topic_name, partition_index)
                            .unwrap_or(0);

                        debug!(
                            topic = %topic_name,
                            partition = partition_index,
                            records_count = records.len(),
                            high_watermark = hw,
                            "Read records from storage"
                        );

                        let records_bytes: Option<Bytes> = if records.is_empty() {
                            None
                        } else {
                            // Pre-calculate total size and get pooled buffer to reduce allocations
                            let total_len: usize = records.iter().map(|r| r.value.len()).sum();
                            let mut buf = self.buffer_pool.get_with_capacity(total_len).await;
                            for record in &records {
                                debug!(
                                    record_offset = record.offset,
                                    record_value_len = record.value.len(),
                                    "Including record in fetch response"
                                );
                                buf.extend_from_slice(&record.value);
                            }
                            Some(buf.freeze())
                        };

                        (records_bytes, hw, NONE)
                    }
                    Err(e) => {
                        debug!(
                            topic = %topic_name,
                            partition = partition_index,
                            error = %e,
                            "Error reading from storage"
                        );
                        // Use proper error code mapping instead of hardcoded UNKNOWN_TOPIC_OR_PARTITION
                        // This returns CORRUPT_MESSAGE for CorruptedData, KAFKA_STORAGE_ERROR for Storage, etc.
                        let error_code = e.kafka_error_code().as_i16();
                        (None, 0, error_code)
                    }
                };

                let log_start_offset = self
                    .topic_manager
                    .earliest_offset(&topic_name, partition_index)
                    .unwrap_or(0);

                // Calculate last_stable_offset and get aborted transactions for READ_COMMITTED isolation
                // For READ_UNCOMMITTED (isolation_level=0), last_stable_offset = high_watermark
                // For READ_COMMITTED (isolation_level=1), last_stable_offset is the first offset
                // of any ongoing transaction, or high_watermark if no ongoing transactions
                let (last_stable_offset, aborted_transactions) = if is_read_committed {
                    let lso = lso_for_read.unwrap_or(high_watermark);

                    // Get aborted transactions within the fetch range for client-side filtering
                    let aborted = if let Some(ref txn_coordinator) = self.transaction_coordinator {
                        let aborted_list = txn_coordinator.get_aborted_transactions(
                            &topic_name,
                            partition_index,
                            fetch_offset,
                            lso,
                        );

                        use kafka_protocol::messages::ProducerId as KafkaProducerId;
                        let kafka_aborted: Vec<AbortedTransaction> = aborted_list
                            .into_iter()
                            .map(|a| {
                                AbortedTransaction::default()
                                    .with_producer_id(KafkaProducerId(a.producer_id))
                                    .with_first_offset(a.first_offset)
                            })
                            .collect();

                        if kafka_aborted.is_empty() {
                            None
                        } else {
                            Some(kafka_aborted)
                        }
                    } else {
                        None
                    };

                    (lso, aborted)
                } else {
                    (high_watermark, None)
                };

                let partition_data = PartitionData::default()
                    .with_partition_index(partition_index)
                    .with_error_code(error_code)
                    .with_high_watermark(high_watermark)
                    .with_last_stable_offset(last_stable_offset)
                    .with_log_start_offset(log_start_offset)
                    .with_aborted_transactions(aborted_transactions)
                    .with_records(records);

                partition_responses.push(partition_data);
            }

            let topic_response = FetchableTopicResponse::default()
                .with_topic(TopicName::from(StrBytes::from_string(topic_name)))
                .with_partitions(partition_responses);

            topic_responses.push(topic_response);
        }

        // Calculate throttle time if quota manager is enabled
        // We estimate consumed bytes from the response
        let throttle_time_ms = if let Some(ref quota_manager) = self.quota_manager {
            let total_bytes: u64 = topic_responses
                .iter()
                .flat_map(|t| t.partitions.iter())
                .filter_map(|p| p.records.as_ref())
                .map(|r| r.len() as u64)
                .sum();

            // Use actual client info for quota tracking
            let peer_ip = session_manager
                .peer_addr()
                .map(|addr| addr.ip())
                .unwrap_or_else(|| std::net::Ipv4Addr::LOCALHOST.into());
            quota_manager.check_consumer_quota(client_id, peer_ip, total_bytes)
        } else {
            0
        };

        let response = FetchResponse::default()
            .with_responses(topic_responses)
            .with_throttle_time_ms(throttle_time_ms);

        Ok(response)
    }

    /// Handle ListOffsets request
    pub(in crate::protocol) fn handle_list_offsets(
        &self,
        request: ListOffsetsRequest,
        api_version: i16,
    ) -> Result<ListOffsetsResponse> {
        use kafka_protocol::messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };

        debug!(
            api_version = api_version,
            topics_count = request.topics.len(),
            "ListOffsets request received"
        );

        let mut topic_responses = Vec::new();

        for topic in request.topics.iter() {
            let topic_name = topic.name.as_str().to_string();
            let mut partition_responses = Vec::new();

            for partition in topic.partitions.iter() {
                let partition_index = partition.partition_index;
                let timestamp = partition.timestamp;

                debug!(
                    topic = %topic_name,
                    partition = partition_index,
                    timestamp = timestamp,
                    "ListOffsets partition request"
                );

                // Timestamp -1 = latest, -2 = earliest
                let (offset, error_code) = match timestamp {
                    -1 => {
                        match self
                            .topic_manager
                            .latest_offset(&topic_name, partition_index)
                        {
                            Ok(o) => (o, NONE),
                            Err(_) => (0, UNKNOWN_TOPIC_OR_PARTITION),
                        }
                    }
                    -2 => {
                        match self
                            .topic_manager
                            .earliest_offset(&topic_name, partition_index)
                        {
                            Ok(o) => (o, NONE),
                            Err(_) => (0, UNKNOWN_TOPIC_OR_PARTITION),
                        }
                    }
                    _ => {
                        // Timestamp-based lookup: find first offset >= timestamp
                        match self.topic_manager.find_offset_by_timestamp(
                            &topic_name,
                            partition_index,
                            timestamp,
                        ) {
                            Ok(Some(offset)) => (offset, NONE),
                            Ok(None) => {
                                // No offset found for timestamp - per Kafka protocol,
                                // return latest offset if no records match
                                match self
                                    .topic_manager
                                    .latest_offset(&topic_name, partition_index)
                                {
                                    Ok(o) => (o, NONE),
                                    Err(_) => (0, UNKNOWN_TOPIC_OR_PARTITION),
                                }
                            }
                            Err(_) => (0, UNKNOWN_TOPIC_OR_PARTITION),
                        }
                    }
                };

                let timestamp_response = chrono::Utc::now().timestamp_millis();

                debug!(
                    topic = %topic_name,
                    partition = partition_index,
                    offset = offset,
                    error_code = error_code,
                    timestamp_response = timestamp_response,
                    "ListOffsets partition response"
                );

                let mut partition_response = ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition_index)
                    .with_error_code(error_code)
                    .with_timestamp(timestamp_response)
                    .with_offset(offset);

                // v4+ requires leader_epoch
                if api_version >= 4 {
                    partition_response = partition_response.with_leader_epoch(0);
                }

                partition_responses.push(partition_response);
            }

            let topic_response = ListOffsetsTopicResponse::default()
                .with_name(TopicName::from(StrBytes::from_string(topic_name)))
                .with_partitions(partition_responses);

            topic_responses.push(topic_response);
        }

        let topics_count = topic_responses.len();
        let mut response = ListOffsetsResponse::default().with_topics(topic_responses);

        // v2+ requires throttle_time_ms
        if api_version >= 2 {
            response = response.with_throttle_time_ms(0);
        }

        debug!(topics_count = topics_count, "ListOffsets response sent");

        Ok(response)
    }

}
