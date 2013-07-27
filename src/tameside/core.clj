(ns tameside.core
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as c]
            [cascalog.vars :as vars]
            [clojure-csv.core :as csv]))

(def all-cols
  (read-string (slurp (clojure.java.io/resource "cols.edn"))))

(defn to-nullable-var [s]
  (str "!" (name s)))

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
             (partial null-or-s ["NA" "N/A" ""]))
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

(deffilterop not-all-null? [& tuples]
  (not (every? nil? tuples)))

(def people-involved
  {:full-time-or-equivalent ["q3_1a" "q3_1b"]
   :student-or-trainee      ["q3_2a" "q3_2b"]
   :volunteer               ["q3_3a"]})

(defn add-ignore-null [& xs]
  (apply +
         (map (fn [x] (or x 0))
              xs)))

(def workforce-breakdown-by-region
  (let [cols (reduce into
                     ["questionnaire"]
                     (map people-involved
                          [:full-time-or-equivalent :student-or-trainee :volunteer]))
        vs (map to-nullable-var cols)
        in (tameside-query cols)]
    (<- [!questionnaire ?all-paid ?all-student ?all-volunteer]
        (in :>> vs)
        (not-all-null? :<< vs)
        (add-ignore-null !q3_1a !q3_1b :> ?paid)
        (add-ignore-null !q3_2a !q3_2b :> ?student)
        (add-ignore-null !q3_3a :> ?volunteer)
        (c/sum ?paid ?student ?volunteer :> ?all-paid ?all-student ?all-volunteer))))

(defmapcatop make-bruce-happy
  [workforce-types & xs]
  (partition 2 (interleave workforce-types xs)))

#_(?- (stdout)
    (<- [?region ?a ?b]
        (workforce-breakdown-by-region ?region ?all-paid ?all-student ?all-volunteer)
        (make-bruce-happy ["Paid" "Student" "Volunteer"] ?all-paid ?all-student ?all-volunteer :> ?a ?b)))
