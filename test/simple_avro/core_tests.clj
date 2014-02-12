(ns simple-avro.core-tests
  (:use (simple-avro schema core)
        (clojure test)))

(deftest test-prim-types
  (is (= (pack avro-null    nil)          nil))
  (is (= (pack avro-null    5)            nil))
  (is (= (pack avro-boolean true)         true))
  (is (= (pack avro-boolean nil)          false))
  (is (= (pack avro-int     5)            5))
  (is (= (pack avro-long    10)           (long 10)))
  (is (= (pack avro-long    (long 10))    (long 10)))
  (is (= (pack avro-float   2.5)          (float 2.5)))
  (is (= (pack avro-float   (float 2.5))  (float 2.5)))
  (is (= (pack avro-double  2.5)          (double 2.5)))
  (is (= (pack avro-double  (double 2.5)) (double 2.5)))
  (is (= (str (pack avro-string  "test")) "test")))


(defn keywordify-keys
  [something]
  (if (map? something)
    (persistent!
     (let [new-map (transient (hash-map))]
       (doseq [keyval (seq something)]
         (assoc! new-map
                 (keyword (key keyval))
                 (keywordify-keys (val keyval))))
       new-map))
    something))

;; Some types
(def bool-array (avro-array avro-boolean))
(def int-map    (avro-map avro-int))
(def a-union    (avro-union avro-string avro-int avro-null))
(def a-n-union ;; union that begins with null
  (avro-union avro-null avro-string avro-int ))

(defavro-fixed MyFixed 2)

(defavro-enum MyEnum "A" "B" "C")

(defavro-record MyRecord
  "f1" avro-int
  "f2" avro-string)

(defavro-record MyNestedRecord
  "f1" avro-int
  "f2" MyRecord
  "f3" MyRecord)

(defavro-record List
  "value" avro-int
  "next"  (avro-union "List" avro-null))

(def recursive
  {"value" 1
   "next"  {"value" 2
            "next"  {"value" 3
                     "next"  nil}}})

(def nested-record
  {"f1" 10
   "f2" {"f1" 20
         "f2" "f2-f2"}
   "f3" {"f1" 20
         "f2" "f3-f2"}})

(def keyword-nested-record
  (keywordify-keys nested-record))

(def simple-int-map  {"a" 1 "b" 2})
(def keyword-int-map (keywordify-keys simple-int-map)) ;; {:a 1 :b 2}


(defn keyword-insensitive-tests
  [encoder decoder keywords]
  (is (= (unpack avro-null (pack avro-null nil  encoder) :decoder decoder :str-keys keywords)       nil))
  (is (= (unpack avro-null    (pack avro-null    5    encoder) :decoder decoder :str-keys keywords) nil))
  (is (= (unpack avro-boolean (pack avro-boolean true encoder) :decoder decoder :str-keys keywords) true))
  (is (= (unpack avro-int     (pack avro-int     5    encoder) :decoder decoder :str-keys keywords) 5))
  (is (= (unpack avro-long    (pack avro-long    10   encoder) :decoder decoder :str-keys keywords) (long 10)))
  (is (= (unpack avro-float   (pack avro-float   2.5  encoder) :decoder decoder :str-keys keywords) (float 2.5)))
  (is (= (unpack avro-double  (pack avro-double  2.5  encoder) :decoder decoder :str-keys keywords) (double 2.5)))
  (is (= (str (unpack avro-string (pack avro-string "test" encoder) :decoder decoder :str-keys keywords))  "test"))

  (is (= (unpack bool-array (pack bool-array [true false false] encoder) :decoder decoder :str-keys keywords) [true false false]))

  (is (= (unpack a-union (pack a-union "test" encoder) :decoder decoder :str-keys keywords) "test"))
  (is (= (unpack a-union (pack a-union 10 encoder) :decoder decoder :str-keys keywords) 10))

  (is (= (unpack a-n-union (pack a-n-union "test" encoder) :decoder decoder :str-keys keywords) "test"))
  (is (= (unpack a-n-union (pack a-n-union 10 encoder) :decoder decoder :str-keys keywords) 10))

  (let [pu (unpack MyFixed (pack MyFixed (byte-array [(byte 1) (byte 2)]) encoder) :decoder decoder :str-keys keywords)]
       (is (= (nth pu 0) 1))
       (is (= (nth pu 1) 2)))

  (is (= (unpack MyEnum (pack MyEnum "A" encoder) :decoder decoder :str-keys keywords) "A"))
  (is (= (unpack MyEnum (pack MyEnum "B" encoder) :decoder decoder :str-keys keywords) "B"))
  (is (= (unpack MyEnum (pack MyEnum "C" encoder) :decoder decoder :str-keys keywords) "C")))

(defn keyword-sensitive-tests
  [encoder decoder]

  (is (= (unpack int-map (pack int-map simple-int-map encoder) :decoder decoder :str-key true) simple-int-map))
  (is (= (unpack int-map (pack int-map simple-int-map encoder) :decoder decoder :str-key false) keyword-int-map))
  (is (= (unpack int-map (pack int-map keyword-int-map encoder) :decoder decoder :str-key true) simple-int-map))
  (is (= (unpack int-map (pack int-map keyword-int-map encoder) :decoder decoder :str-key false) keyword-int-map))

  (is (= (unpack List (pack List recursive encoder) :decoder decoder :str-key true) recursive))
  (is (= (unpack List (pack List (keywordify-keys recursive) encoder) :decoder decoder :str-key false) (keywordify-keys recursive)))
  (is (= (unpack List (pack List recursive encoder) :decoder decoder :str-key false) (keywordify-keys recursive)))
  (is (= (unpack List (pack List (keywordify-keys recursive) encoder) :decoder decoder :str-key true) recursive))

  (let [pu (unpack MyRecord (pack MyRecord {"f1" 6 "f2" "test"} encoder) :decoder decoder :str-key false)]
    (is (= (pu :f1) 6))
    (is (= (pu :f2) "test")))
  (let [pu (unpack MyRecord (pack MyRecord {:f1 6 :f2 "test"} encoder) :decoder decoder :str-key false)]
    (is (= (pu :f1) 6))
    (is (= (pu :f2) "test")))
  (let [pu (unpack MyRecord (pack MyRecord {"f1" 6 "f2" "test"} encoder) :decoder decoder :str-key true)]
    (is (= (pu "f1") 6))
    (is (= (pu "f2") "test")))
  (let [pu (unpack MyRecord (pack MyRecord {:f1 6 :f2 "test"} encoder) :decoder decoder :str-key true)]
    (is (= (pu "f1") 6))
    (is (= (pu "f2") "test")))

  ;; Record tests with and without keywords
  (let [results [nested-record
                 {"f1" 10}
                 {"f1" 10 "f2" {"f1" 20 "f2" "f2-f2"}}
                 {"f3" {"f2" "f3-f2"}}
                 {"f1" 10 "f3" {"f2" "f3-f2"}}]
        key-results (vec (map keywordify-keys results))]
    ;; string-keyed record, keyword unpacking
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false) (key-results 0)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields [:f1]) (key-results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields [:f1 :f2]) (key-results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields [[:f3 :f2]]) (key-results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields [:f1 [:f3 :f2]]) (key-results 4)))
    ;; keyword-keyed record, keyword unpacking
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false) (key-results 0)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields [:f1]) (key-results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields [:f1 :f2]) (key-results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields [[:f3 :f2]]) (key-results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields [:f1 [:f3 :f2]]) (key-results 4)))
    ;; string-keyed record, string unpacking
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true) (results 0)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields [:f1]) (results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields [:f1 :f2]) (results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields [[:f3 :f2]]) (results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields [:f1 [:f3 :f2]]) (results 4)))
    ;; keyword-keyed record, string unpacking
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true) (results 0)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields [:f1]) (results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields [:f1 :f2]) (results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields [[:f3 :f2]]) (results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields [:f1 [:f3 :f2]]) (results 4)))

    ;; ## Test the fields specified as strings
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields ["f1"]) (key-results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields ["f1" "f2"]) (key-results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields [["f3" "f2"]]) (key-results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key false :fields ["f1" ["f3" "f2"]]) (key-results 4)))
    ;; keyword-keyed record, keyword unpacking
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields ["f1"]) (key-results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields ["f1" "f2"]) (key-results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields [["f3" "f2"]]) (key-results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key false :fields ["f1" ["f3" "f2"]]) (key-results 4)))
    ;; string-keyed record, string unpacking
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields ["f1"]) (results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields ["f1" "f2"]) (results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields [["f3" "f2"]]) (results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord nested-record encoder) :decoder decoder :str-key true :fields ["f1" ["f3" "f2"]]) (results 4)))
    ;; keyword-keyed record, string unpacking
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields ["f1"]) (results 1)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields ["f1" "f2"]) (results 2)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields [["f3" "f2"]]) (results 3)))
    (is (= (unpack MyNestedRecord (pack MyNestedRecord keyword-nested-record encoder) :decoder decoder :str-key true :fields ["f1" ["f3" "f2"]]) (results 4))))
)

(defmacro test-pack-unpack
  [name encoder decoder]
  `(deftest ~name
     (keyword-insensitive-tests ~encoder ~decoder true)
     (keyword-insensitive-tests ~encoder ~decoder false)
     (keyword-sensitive-tests ~encoder ~decoder)))


(test-pack-unpack test-prim-types-pack-unpack-no-decoder nil nil)
(test-pack-unpack test-prim-types-pack-unpack-json json-encoder json-decoder)
(test-pack-unpack test-prim-types-pack-unpack-binary binary-encoder binary-decoder)
