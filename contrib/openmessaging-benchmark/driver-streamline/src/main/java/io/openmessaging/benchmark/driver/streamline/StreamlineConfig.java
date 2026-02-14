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

/**
 * Configuration for the Streamline benchmark driver.
 *
 * <p>This class is deserialized from the driver YAML configuration file
 * (e.g., streamline.yaml).
 */
public class StreamlineConfig {

    /**
     * Streamline server address in the format host:port.
     * Can be overridden with STREAMLINE_BOOTSTRAP_SERVERS environment variable.
     */
    public String bootstrapServers = "localhost:9092";

    /**
     * Replication factor for topics created during benchmarks.
     * For single-node Streamline, this should be 1.
     */
    public int replicationFactor = 1;

    /**
     * Number of partitions per topic.
     * If 0, uses the workload-specified partition count.
     */
    public int topicPartitions = 0;

    /**
     * Common Kafka client configuration properties.
     * Applied to both producers and consumers.
     */
    public String commonConfig = "";

    /**
     * Producer-specific configuration properties.
     */
    public String producerConfig = "";

    /**
     * Consumer-specific configuration properties.
     */
    public String consumerConfig = "";
}
