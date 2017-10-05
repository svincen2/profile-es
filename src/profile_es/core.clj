(ns profile-es.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [clj-util.maps :as maps])
  (:gen-class))

(def search-api-url
  {:protocol  "http"
   :domain    "localhost"
   :port      nil
   :path      "api/search/search.json"})

(defn url->http-get
  [{:keys [protocol domain port path query-str query-params]}]
  (let [url (str (or protocol "http") "://"
                 domain
                 (if port (str ":" port))
                 (if path (str "/" path))
                 (if query-str (str "?" query-str)))]
    {:method :get :url url :query-params query-params}))

(def progress (atom {:processed 0 :total 0}))

(defn average
  [n f]
  (float (/ (reduce + (repeatedly n f)) n)))

(defn try-parse-json-body
  [response]
  (let [body (:body response)]
    (try
      (json/read-str body)
      (catch Exception e
        (do (println "JSON parse error. response: " response) {})))))

(defn extract-ES-performance
  [response]
  (as-> response r
        (try-parse-json-body r)
        (get-in r ["performance" "elasticsearch"])))

(defn sec-since
  [start-time]
  (float (/ (- (System/currentTimeMillis) start-time) 1000)))

(defn profile
  [sample-size [k & vs] query-str]
  (let [t0 (System/currentTimeMillis)
        base-url (assoc search-api-url :query-str query-str)
        reqs (map url->http-get (maps/copy-assoc base-url k vs))
        fns (map #(fn [] (extract-ES-performance @(http/request %))) reqs)
        profiles (doall (map #(average sample-size %) fns))]
    (swap! progress update :processed inc)
    (println "processed" (:processed @progress) "of" (:total @progress)
             " -- time" (sec-since t0))
    (apply merge
           {:query query-str}
           (map hash-map vs profiles))))

(defn overall-average
  [results k]
  (str "average " k " "
    (as-> results r
      (map #(get % k) r)
      (reduce + r)
      (/ r (count results))
      (float r))))

(defn -main
  [& args]
  (let [sample-size (Integer/parseInt (first args))
        param (map read-string (rest args))
        queries (line-seq (io/reader *in*))
        results (map (partial profile sample-size param) queries)]
    (println "sample size" sample-size)
    (println "beginning profile...")
    (swap! progress assoc :total (count queries))
    (doall
      (map println
           (map (partial overall-average results) (rest param))))))

