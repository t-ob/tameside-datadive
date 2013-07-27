(ns tameside.core
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as c]
            [cascalog.vars :as vars]
            [clojure-csv.core :as csv]))

(def all-cols
  (read-string (slurp (clojure.java.io/resource "cols.edn"))))

(defn to-nullable-var [s]
  (str "!" (name s)))

#_(def nullable-cols
  (map (partial str "!")
       cols))

(defn null-or-s [null-strings s]
  (if (contains? (into #{}
                       null-strings)
                 s)
    nil
    s))

(defn numeric-or-s [s]
  (when s
    (if (re-matches #"^\d+[.]?\d*$"
                    s)
      (Double/parseDouble s)
      s)))

(defn parse-line [s]
  (map (comp numeric-or-s
             (partial null-or-s ["NA" ""]))
       (first (csv/parse-csv s))))

(def column-partition-map
  (group-by (comp (partial apply str)
                  (partial take 2))
            all-cols))

(defn tameside-query
  ([]
     (tameside-query all-cols))
  ([cols]
     (let [in-file  "./input/tameside-master.csv"
           in-vars  (map to-nullable-var
                         all-cols)
           out-vars (map to-nullable-var
                         cols)]
       (<- out-vars
           ((lfs-textline in-file) ?line)
           (parse-line ?line :>> in-vars)))))

#_(?- (stdout)
    (c/first-n (tameside-query ["q6_3a_Tameside" "q6_3b_Tameside" "q6_3bf" "q6_3bg" "q6_3a_GM" "q6_3b_GM"])
               10))

(def q1_9-group
  ["q1_9a_Bury" "q1_9b_Bolton" "q1_9c_Manchester" "q1_9d_Oldham" "q1_9e_Rochdale" "q1_9f_Salford" "q1_9g_Stockport" "q1_9h_Tameside" "q1_9i_Trafford" "q1_9j_Wigan"])

(def q1_10-group
  ["q1_10a_Bury" "q1_10b_Bolton" "q1_10c_Manchester" "q1_10d_Oldham" "q1_10e_Rochdale" "q1_10f_Salford" "q1_10g_Stockport" "q1_10h_Tameside" "q1_10i_Trafford" "q1_10j_Wigan"])
