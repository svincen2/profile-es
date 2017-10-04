(ns profile-es.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http])
  (:gen-class))

(def progress (atom {:processed 0 :total 0}))

(defn average
  [n f]
  (float (/ (reduce + (repeatedly n f)) n)))

(defn search
  [port query-str]
  (let [url (str "http://localhost:" port "/api/search/search.json?" query-str)]
    @(http/get url)))

(defn try-parse-json-body
  [response]
  (let [body (:body response)]
    (try
      (json/read-str body)
      (catch Exception e (do (println "JSON parse error. response: " response) {})))))

(defn extract-ES-performance
  [response]
  (as-> response r
        (try-parse-json-body r)
        (get-in r ["performance" "elasticsearch"])))

(defn sec-since
  [start-time]
  (float (/ (- (System/currentTimeMillis) start-time) 1000)))

(defn profile
  [sample-size query-str]
  (let [t0 (System/currentTimeMillis)
        ave-80 (average sample-size #(extract-ES-performance (search 80 query-str)))
        ave-81 (average sample-size #(extract-ES-performance (search 81 query-str)))]
    (swap! progress update :processed inc)
    (println "processed" (:processed @progress) "of" (:total @progress)
             " -- time" (sec-since t0))
    {:query query-str
     :81 ave-81
     :80 ave-80}))

(defn -main
  [& args]
  (let [sample-size (Integer/parseInt (first args))
        queries (line-seq (io/reader *in*))
        results (map (partial profile sample-size) queries)]
    (println "sample size" sample-size)
    (println "beginning profile...")
    (swap! progress assoc :total (count queries))
    (println "average (81)"
             (as-> results r
                   (map :81 r)
                   (reduce + r)
                   (/ r (count queries))
                   (float r)))
    (println "average (80)"
             (as-> results r
                   (map :80 r)
                   (reduce + r)
                   (/ r (count queries))
                   (float r)))))

