(ns jepsen.streamline.workload
  "Jepsen workloads for Streamline testing"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.streamline.client :as client]
            [knossos.model :as model]))

;; Append-only workload - tests linearizability of appends
(defn append-gen
  "Generator for append operations"
  [opts]
  (let [key-count   (:key-count opts 10)
        ops-per-key (:ops-per-key opts 100)]
    (->> (range key-count)
         (map (fn [k]
                (->> (range ops-per-key)
                     (map (fn [v] {:type :invoke
                                   :f    :append
                                   :value {:key k :val v}})))))
         (apply gen/mix)
         (gen/stagger (/ 1 (:rate opts 10))))))

(defn append-checker
  "Checker for append workload"
  []
  (checker/compose
   {:timeline (timeline/html)
    :stats    (checker/stats)
    :linear   (checker/linearizable
               {:model     (model/unordered-queue)
                :algorithm :linear})}))

;; Queue workload - tests FIFO ordering
(defn queue-gen
  "Generator for queue operations (enqueue/dequeue)"
  [opts]
  (let [enqueue-ratio 0.6]
    (->> (gen/mix [(gen/repeat {:type :invoke :f :append :value {:key 0 :val (rand-int 1000)}})
                   (gen/repeat {:type :invoke :f :read})])
         (gen/stagger (/ 1 (:rate opts 10))))))

(defn queue-checker
  "Checker for queue workload"
  []
  (checker/compose
   {:timeline (timeline/html)
    :stats    (checker/stats)
    :queue    (checker/linearizable
               {:model     (model/unordered-queue)
                :algorithm :linear})}))

;; Register workload - tests single-key linearizability
(defn register-gen
  "Generator for register operations (read/write)"
  [opts]
  (->> (gen/mix [(gen/repeat {:type :invoke :f :read})
                 (gen/repeat {:type :invoke :f :append :value {:key 0 :val (rand-int 1000)}})])
       (gen/stagger (/ 1 (:rate opts 10)))))

(defn register-checker
  "Checker for register workload"
  []
  (checker/compose
   {:timeline (timeline/html)
    :stats    (checker/stats)
    :linear   (checker/linearizable
               {:model     (model/cas-register)
                :algorithm :linear})}))

;; Nemesis definitions
(defn nemesis-package
  "Create nemesis based on configuration"
  [opts]
  (let [faults (set (:faults opts [:partition :kill :pause]))]
    {:nemesis (nc/nemesis-package
               {:db        (:db opts)
                :nodes     (:nodes opts)
                :faults    faults
                :partition {:targets [:majority :one]}
                :kill      {:targets [:one :all]}
                :pause     {:targets [:one]}})
     :generator (->> [(when (:partition faults)
                        (gen/repeat {:type :info :f :partition :value :majority}))
                      (when (:kill faults)
                        (gen/repeat {:type :info :f :kill :value :one}))
                      (when (:pause faults)
                        (gen/repeat {:type :info :f :pause :value :one}))]
                     (remove nil?)
                     (apply gen/mix)
                     (gen/stagger 10))}))

(defn workload
  "Create a workload based on type"
  [workload-type opts]
  (let [base {:client  (client/client opts)
              :nemesis nemesis/noop}]
    (merge base
           (case workload-type
             :append
             {:generator (append-gen opts)
              :checker   (append-checker)}

             :queue
             {:generator (queue-gen opts)
              :checker   (queue-checker)}

             :register
             {:generator (register-gen opts)
              :checker   (register-checker)}

             ;; Default to append
             {:generator (append-gen opts)
              :checker   (append-checker)}))))
