(ns com.yolodata.tbana.storm.splunk-integration-test
  (:use [clojure test])
  (:use [backtype.storm testing])
  (:use [com.yolodata.tbana.storm.splunk]))

(def serviceArgs {"username" "admin",
                  "password" "changeIt",
                  "host" "localhost",
                  "port" (int 9050)})

(def exportArgs {"earliest_time" "0",
                 "latest_time" "now",
                 "search_mode" "normal"})

(defn mk-topology []
  (topology
    {"1" (spout-spec (splunk-csv-spout serviceArgs "search sourcetype=mock" exportArgs)
           :p 1)}
    {"2" (bolt-spec {"1" :shuffle}
           split-event
           :p 1)}))

(deftest splunk-spout-test
  (with-local-cluster [cluster]
    (.submitTopology cluster "splunk-integration-test" {TOPOLOGY-DEBUG true} (mk-topology))))