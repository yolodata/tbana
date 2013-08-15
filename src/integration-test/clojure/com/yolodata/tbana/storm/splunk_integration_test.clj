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