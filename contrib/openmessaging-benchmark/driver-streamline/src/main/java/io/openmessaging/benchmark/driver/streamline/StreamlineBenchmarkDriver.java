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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark driver for Streamline.
 *
 * <p>Streamline is a Kafka-compatible streaming platform, so this driver uses the
 * standard Kafka client libraries. The driver is optimized for Streamline's
 * characteristics: low latency, efficient storage, and simple deployment.
 *
 * <p>Usage:
 * <pre>
 * bin/benchmark --drivers driver-streamline/streamline.yaml \
 *               --workloads workloads/streamline-quick.yaml
 * </pre>
 */
public class StreamlineBenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(StreamlineBenchmarkDriver.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private StreamlineConfig config;
    private AdminClient adminClient;

    private final List<BenchmarkProducer> producers = Collections.synchronizedList(new ArrayList<>());
    private final List<BenchmarkConsumer> consumers = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void initialize(File configurationFile, org.apache.bookkeeper.stats.StatsLogger statsLogger)
            throws IOException {
        log.info("Initializing Streamline benchmark driver");

        config = mapper.readValue(configurationFile, StreamlineConfig.class);

        // Allow environment variable override for bootstrap servers
        String envServers = System.getenv("STREAMLINE_BOOTSTRAP_SERVERS");
        if (envServers != null && !envServers.isEmpty()) {
            config.bootstrapServers = envServers;
            log.info("Using bootstrap servers from environment: {}", envServers);
        }

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", config.bootstrapServers);
        adminProps.putAll(parseConfig(config.commonConfig));

        adminClient = AdminClient.create(adminProps);
        log.info("Connected to Streamline at {}", config.bootstrapServers);
    }

    @Override
    public String getTopicNamePrefix() {
        return "streamline-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        int numPartitions = config.topicPartitions > 0 ? config.topicPartitions : partitions;
        log.info("Creating topic {} with {} partitions", topic, numPartitions);

        NewTopic newTopic = new NewTopic(topic, numPartitions, (short) config.replicationFactor);

        return adminClient.createTopics(Collections.singletonList(newTopic))
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .thenAccept(v -> log.info("Created topic: {}", topic));
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.putAll(parseConfig(config.commonConfig));
        props.putAll(parseConfig(config.producerConfig));

        StreamlineBenchmarkProducer producer = new StreamlineBenchmarkProducer(props, topic);
        producers.add(producer);

        return CompletableFuture.completedFuture(producer);
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic,
            String subscriptionName,
            ConsumerCallback callback) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, subscriptionName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.putAll(parseConfig(config.commonConfig));
        props.putAll(parseConfig(config.consumerConfig));

        StreamlineBenchmarkConsumer consumer = new StreamlineBenchmarkConsumer(props, topic, callback);
        consumers.add(consumer);

        return CompletableFuture.completedFuture(consumer);
    }

    @Override
    public void close() throws Exception {
        log.info("Closing Streamline benchmark driver");

        for (BenchmarkProducer producer : producers) {
            producer.close();
        }
        producers.clear();

        for (BenchmarkConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();

        if (adminClient != null) {
            adminClient.close();
        }

        log.info("Streamline benchmark driver closed");
    }

    private Properties parseConfig(String config) {
        Properties props = new Properties();
        if (config != null && !config.trim().isEmpty()) {
            try {
                props.load(new StringReader(config));
            } catch (IOException e) {
                log.warn("Failed to parse config: {}", e.getMessage());
            }
        }
        return props;
    }
}
