(ns jepsen.streamline.client
  "Kafka client for Streamline"
  (:require [clojure.tools.logging :refer [info warn error]]
            [jepsen.client :as client])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig ConsumerRecords]
           [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]
           [java.time Duration]
           [java.util Properties Collections]))

(defn producer-config
  "Create Kafka producer configuration"
  [bootstrap-servers]
  (doto (Properties.)
    (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG StringSerializer)
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG StringSerializer)
    (.put ProducerConfig/ACKS_CONFIG "all")
    (.put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG "true")
    (.put ProducerConfig/MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION "5")
    (.put ProducerConfig/RETRIES_CONFIG "3")))

(defn consumer-config
  "Create Kafka consumer configuration"
  [bootstrap-servers group-id]
  (doto (Properties.)
    (.put ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
    (.put ConsumerConfig/GROUP_ID_CONFIG group-id)
    (.put ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG StringDeserializer)
    (.put ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG StringDeserializer)
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")
    (.put ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG "false")
    (.put ConsumerConfig/ISOLATION_LEVEL_CONFIG "read_committed")))

(defn admin-config
  "Create Kafka admin client configuration"
  [bootstrap-servers]
  (doto (Properties.)
    (.put AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)))

(defn create-topic!
  "Create a topic using admin client"
  [admin topic-name partitions replication-factor]
  (let [topic (NewTopic. topic-name partitions (short replication-factor))]
    (.get (.createTopics admin (Collections/singleton topic)))))

(defn send-record!
  "Send a record and wait for acknowledgment"
  [producer topic key value]
  (let [record (ProducerRecord. topic key value)
        future (.send producer record)]
    (.get future)))

(defn poll-records
  "Poll for records with timeout"
  [consumer timeout-ms]
  (let [records (.poll consumer (Duration/ofMillis timeout-ms))]
    (for [record records]
      {:topic     (.topic record)
       :partition (.partition record)
       :offset    (.offset record)
       :key       (.key record)
       :value     (.value record)})))

(defrecord StreamlineClient [bootstrap-servers
                             producer
                             consumer
                             admin
                             topic
                             partitions
                             replication-factor]
  client/Client
  (open! [this test node]
    (let [servers (str node ":9092")]
      (info "Opening client to" servers)
      (assoc this
             :bootstrap-servers servers
             :producer (KafkaProducer. (producer-config servers))
             :consumer (KafkaConsumer. (consumer-config servers "jepsen-consumer"))
             :admin    (AdminClient/create (admin-config servers)))))

  (setup! [this test]
    (info "Setting up topic" topic)
    (try
      (create-topic! admin topic partitions replication-factor)
      (catch Exception e
        (warn "Topic may already exist:" (.getMessage e))))
    (.subscribe consumer (Collections/singleton topic))
    this)

  (invoke! [this test op]
    (case (:f op)
      :append
      (try
        (let [k     (:key (:value op))
              v     (:val (:value op))
              result (send-record! producer topic (str k) (str v))]
          (assoc op
                 :type :ok
                 :value {:key    k
                         :val    v
                         :offset (.offset result)
                         :partition (.partition result)}))
        (catch Exception e
          (assoc op :type :fail :error (.getMessage e))))

      :read
      (try
        (let [records (poll-records consumer 1000)]
          (assoc op
                 :type :ok
                 :value (map (fn [r] {:key   (Long/parseLong (:key r))
                                      :val   (Long/parseLong (:value r))
                                      :offset (:offset r)})
                             records)))
        (catch Exception e
          (assoc op :type :fail :error (.getMessage e))))))

  (teardown! [this test]
    (info "Tearing down client"))

  (close! [this test]
    (info "Closing client")
    (when producer (.close producer))
    (when consumer (.close consumer))
    (when admin (.close admin))))

(defn client
  "Create a Streamline client"
  [opts]
  (map->StreamlineClient
   {:topic              "jepsen-test"
    :partitions         (:partitions opts)
    :replication-factor (:replication-factor opts)}))
