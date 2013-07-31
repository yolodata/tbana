(ns com.yolodata.tbana.storm.splunk
  (:import (com.splunk Service ServiceArgs JobExportArgs))
  (:require [opencsv-clj.core :as csv])
  (:use [backtype.storm clojure config])
  (:gen-class :main true))

(defn connectToSplunk [serviceArgs]
  (Service/connect serviceArgs))

(defn createExportStream [service query exportArgs]
  (.export service query exportArgs))

(defspout splunk-csv-spout ["event"] {:params [serviceArgs splunkQuery exportArgs]}
  [conf contex collector]
  (let [service (connectToSplunk serviceArgs)
        exportArgsWithCSV (conj exportArgs {"output_mode" "csv"}) ;;overrides output format to csv
        exportStream (createExportStream service splunkQuery exportArgsWithCSV)
        csvSeq (atom (csv/parse (java.io.InputStreamReader. exportStream) :mapped true))]
    ;;(do (println "***\n***\n" (first csvSeq) "\n***\n***\n"))
    (spout
      (nextTuple []
        (Thread/sleep 500)
        (emit-spout! collector [(first (swap! csvSeq rest))]))
      (ack [id]
        ;;TODO: FIX THIS
        ))))

(defbolt split-event ["key" "value"] [tuple collector]
  (let [values (.getValue tuple 0)]
    (doseq [pair values]
      (emit-bolt! collector pair :anchor tuple))
    (ack! collector tuple)
    ))

