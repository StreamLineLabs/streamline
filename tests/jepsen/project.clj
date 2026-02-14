(defproject jepsen.streamline "0.2.0-SNAPSHOT"
  :description "Jepsen tests for Streamline"
  :url "https://github.com/josedab/streamline"
  :license {:name "Apache-2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.12.0"]
                 [io.confluent/kafka-clients "7.5.0"]]
  :main jepsen.streamline.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
