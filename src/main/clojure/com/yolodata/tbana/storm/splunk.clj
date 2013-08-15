;
; Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
;
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
; http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;

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

