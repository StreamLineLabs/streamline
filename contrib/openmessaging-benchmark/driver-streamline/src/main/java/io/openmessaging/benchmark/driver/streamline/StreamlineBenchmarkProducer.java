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

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark producer for Streamline.
 *
 * <p>Uses the standard Kafka producer client since Streamline is Kafka-compatible.
 */
public class StreamlineBenchmarkProducer implements BenchmarkProducer {

    private static final Logger log = LoggerFactory.getLogger(StreamlineBenchmarkProducer.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;

    public StreamlineBenchmarkProducer(Properties props, String topic) {
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        log.debug("Created producer for topic: {}", topic);
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                topic,
                key.map(String::getBytes).orElse(null),
                payload);

        CompletableFuture<Void> future = new CompletableFuture<>();

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(null);
            }
        });

        return future;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing producer for topic: {}", topic);
        producer.close();
    }
}
