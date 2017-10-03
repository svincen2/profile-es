(ns profile-es.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http])
  (:gen-class))

(defn average
  [n f]
  (float (/ (reduce + (repeatedly n f)) n)))

(defn search
  [port query-str]
  (let [url (str "http://localhost:" port "/api/search/search.json?" query-str)]
    @(http/get url)))

(defn extract-ES-performance
  [response]
  (as-> response r
        (:body r)
        (json/read-str r)
        (get-in r ["performance" "elasticsearch"])))

(defn profile
  [query-str]
  {:query query-str
   :81 (average 100 #(extract-ES-performance (search 80 query-str)))
   :80 (average 100 #(extract-ES-performance (search 81 query-str)))})

(defn -main
  [& args]
  (let [queries (line-seq (io/reader *in*))
        results (map profile queries)]
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

