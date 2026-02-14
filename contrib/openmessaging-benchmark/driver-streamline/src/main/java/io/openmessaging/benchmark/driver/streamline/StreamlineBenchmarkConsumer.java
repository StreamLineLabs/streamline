/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.streamline;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark consumer for Streamline.
 *
 * <p>Uses the standard Kafka consumer client since Streamline is Kafka-compatible.
 * Runs a polling loop in a background thread and delivers messages via callback.
 */
public class StreamlineBenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(StreamlineBenchmarkConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public StreamlineBenchmarkConsumer(Properties props, String topic, ConsumerCallback callback) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));

        log.debug("Created consumer for topic: {}", topic);

        // Start polling thread
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "streamline-consumer-" + topic);
            t.setDaemon(true);
            return t;
        });

        executor.submit(() -> {
            try {
                while (running.get()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        callback.messageReceived(record.value(), record.timestamp());
                    }

                    if (!records.isEmpty()) {
                        consumer.commitAsync();
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    log.error("Consumer error", e);
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing consumer");
        running.set(false);
        executor.shutdown();
        consumer.close();
    }
}
