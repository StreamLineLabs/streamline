(ns jepsen.streamline.core
  "Entry point for Streamline Jepsen tests"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.cli :as cli]
            [jepsen.tests :as tests]
            [jepsen.streamline.db :as db]
            [jepsen.streamline.workload :as workload]))

(def cli-opts
  "Additional CLI options for Streamline tests"
  [[nil "--version VERSION" "Streamline version to test"
    :default "latest"]
   [nil "--rate RATE" "Approximate operations per second"
    :default 10
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "Must be a positive number"]]
   [nil "--ops-per-key OPS" "Operations per key"
    :default 100
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]
   [nil "--workload WORKLOAD" "Workload: append, queue, or register"
    :default "append"
    :validate [#{"append" "queue" "register"} "Must be append, queue, or register"]]
   [nil "--partitions PARTS" "Number of partitions per topic"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]
   [nil "--replication-factor RF" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]])

(defn streamline-test
  "Construct a Jepsen test for Streamline"
  [opts]
  (let [workload-name (keyword (:workload opts))
        workload      (workload/workload workload-name opts)
        db            (db/db (:version opts))]
    (merge tests/noop-test
           opts
           {:name       (str "streamline-" (name workload-name))
            :os         jepsen.os/noop
            :db         db
            :client     (:client workload)
            :nemesis    (:nemesis workload)
            :checker    (:checker workload)
            :generator  (:generator workload)
            :pure-generators true})))

(defn -main
  "Entry point for running Streamline Jepsen tests"
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn  streamline-test
                                :opt-spec cli-opts})
          (cli/serve-cmd))
   args))
