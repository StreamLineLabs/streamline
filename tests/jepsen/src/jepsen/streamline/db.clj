(ns jepsen.streamline.db
  "Database lifecycle for Streamline"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]))

(def install-dir "/opt/streamline")
(def data-dir "/var/lib/streamline")
(def log-file "/var/log/streamline.log")
(def pid-file "/var/run/streamline.pid")

(defn node-id
  "Get numeric node ID for a node name"
  [test node]
  (.indexOf (:nodes test) node))

(defn seed-nodes
  "Get comma-separated list of seed nodes"
  [test]
  (->> (:nodes test)
       (map #(str % ":9093"))
       (clojure.string/join ",")))

(defn start-streamline!
  "Start Streamline on a node"
  [test node]
  (info node "Starting Streamline")
  (c/su
   (cu/start-daemon!
    {:logfile log-file
     :pidfile pid-file
     :chdir   install-dir}
    (str install-dir "/streamline")
    :--node-id (node-id test node)
    :--listen-addr (str node ":9092")
    :--inter-broker-addr (str node ":9093")
    :--seed-nodes (seed-nodes test)
    :--data-dir data-dir
    :--log-level "info")))

(defn stop-streamline!
  "Stop Streamline on a node"
  [node]
  (info node "Stopping Streamline")
  (c/su
   (cu/stop-daemon! pid-file)))

(defn db
  "Streamline database for Jepsen"
  [version]
  (reify db/DB
    (setup! [this test node]
      (info node "Setting up Streamline" version)
      (c/su
       ;; Create directories
       (c/exec :mkdir :-p install-dir)
       (c/exec :mkdir :-p data-dir)

       ;; Download or copy binary
       ;; For local testing, assume binary is already present
       ;; In CI, download from GitHub releases
       (when (not (cu/file? (str install-dir "/streamline")))
         (info node "Downloading Streamline binary...")
         (let [url (str "https://github.com/josedab/streamline/releases/download/"
                        version "/streamline-linux-amd64")]
           (c/exec :curl :-L :-o (str install-dir "/streamline") url)
           (c/exec :chmod :+x (str install-dir "/streamline")))))

      ;; Start the service
      (start-streamline! test node)

      ;; Wait for startup
      (Thread/sleep 5000))

    (teardown! [this test node]
      (info node "Tearing down Streamline")
      (stop-streamline! node)
      (c/su
       (c/exec :rm :-rf data-dir)
       (c/exec :rm :-f log-file)))

    db/LogFiles
    (log-files [this test node]
      [log-file])))
