(ns com.yolodata.tbana.storm.storm-test
  (:use [clojure test])
  (:use [backtype.storm testing]))

(deftest splunk-spout-test
  (with-local-cluster [cluster]
    (Thread/sleep 500)))