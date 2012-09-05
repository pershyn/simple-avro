(ns simple-avro.core
  {:doc "Core namespace defines serialization/de-serialization functions."}
  (:require (clojure.data [json :as json]))
  (:import (java.io FileOutputStream ByteArrayOutputStream ByteArrayInputStream)
           (org.apache.avro Schema Schema$Type Schema$Field)
           (org.apache.avro.generic GenericData$EnumSymbol
                                    GenericData$Fixed
                                    GenericData$Array
                                    GenericData$Record
                                    GenericDatumWriter
                                    GenericDatumReader)
           (org.apache.avro.io DecoderFactory EncoderFactory)
           (org.apache.avro.util Utf8)))

;
; Encoding 
;

; Typabe protocol to convert custom types to Avro compatible format,
; currenty only record supported (see api.clj).
(defprotocol AvroTypeable
  (avro-pack [this] "Pack custom type into avro."))

; Default implementations
(extend-type Object
  AvroTypeable
    (avro-pack [this] this))
    
(extend-type nil
  AvroTypeable
    (avro-pack [_] nil))

(declare pack avro-schema)

(defmacro throw-with-log
  "Throw exception and log helper."
  {:private true}
  [ & msg ]
  `(let [l#  ~(last msg)]
     (if (instance? Exception l#)
       (throw (Exception. (apply str ~(vec (butlast msg))) l#))
       (throw (Exception. (str ~@msg))))))

;;
;; Packing multimethod
;; 
(defmulti pack-obj
  "Packs an object for the particular schema-type"
  (fn pack-obj-dispatch [#^Schema schema obj]
    (.getType schema)))

(defmethod pack-obj Schema$Type/NULL
  [#^Schema schema obj]
  nil)

(defmethod pack-obj Schema$Type/BOOLEAN
  [#^Schema schema obj]
  (boolean obj))

(defmethod pack-obj Schema$Type/INT
  [#^Schema schema obj]
  (int obj))

(defmethod pack-obj Schema$Type/LONG
  [#^Schema schema obj]
  (long obj))

(defmethod pack-obj Schema$Type/FLOAT
  [#^Schema schema obj]
  (float obj))

(defmethod pack-obj Schema$Type/DOUBLE
  [#^Schema schema obj]
  (double obj))

(defmethod pack-obj Schema$Type/BYTES
  [#^Schema schema obj]
  (bytes obj))
  
(defmethod pack-obj Schema$Type/STRING
  [#^Schema schema obj]
  (if (string? obj)
    (Utf8. (str obj))
    (throw (Exception. (str "'" obj "' is not a string.")))))

(defmethod pack-obj Schema$Type/FIXED
  [#^Schema schema obj]
  (doto (GenericData$Fixed. schema) (.bytes obj)))

(defmethod pack-obj Schema$Type/ENUM
  [#^Schema schema obj] 
  (if-let [enum (some #{obj} (.getEnumSymbols schema))]
    (GenericData$EnumSymbol. schema enum)
    (throw-with-log "Enum does not define '" obj "'.")))

(defmethod pack-obj Schema$Type/UNION
  [#^Schema schema obj] 
  (loop [schemas (seq (.getTypes schema))]
    (if (empty? schemas)
      (throw-with-log "No union type defined for object '" obj "'.")
      (let [rec (try
                  (pack (first schemas) obj)
                  (catch Exception e :not-matching-untion-type))] 
        (if (not= rec :not-matching-untion-type)
          rec
          (recur (next schemas)))))))

(defmethod pack-obj Schema$Type/ARRAY
  [#^Schema schema obj] 
  (let [type-schema (.getElementType schema)
        array       (GenericData$Array. (count obj) schema)]
    (doseq [e obj] (.add array (pack type-schema e)))
    array))

(defmethod pack-obj Schema$Type/MAP
  [#^Schema schema obj] 
  (let [type-schema (.getValueType schema)]
    (reduce (fn [m [k v]] (assoc m k (pack type-schema v))) {} obj)))

(defmethod pack-obj Schema$Type/RECORD
  [#^Schema schema obj]
  (if-let [ks (keys obj)]
    (try
      (let [record (GenericData$Record. schema)]
        (doseq [#^String k ks]
          (let [field (.getField schema (name k))]
            (when (nil? field)
              (throw (Exception. (str "Null field " k " schema " schema))))
            (.put record (name k) (pack (.schema field) (obj k)))))
        record)
      (catch Exception e
        (throw (Exception. (str ">>> " schema " - " obj) e))))))

(defmethod pack-obj :default
  [#^Schema schema obj]
  (throw-with-log "No pack defined for type '" (.getType schema) "'."))


(defn- encode-to
  [#^Schema schema obj encoder result]
  (let [stream  (ByteArrayOutputStream.)
        writer  (GenericDatumWriter. schema)
        #^java.io.Flushable encoder (encoder schema stream)]
    (.write writer obj encoder)
    (.flush encoder)
    (result stream)))

(defn pack
  [schema obj & [encoder]]
  (let [#^Schema schema (avro-schema schema)
                 encode (or encoder (fn [_ obj] obj))
                 obj    (avro-pack obj)]
    (try
      (encode schema (pack-obj schema obj))
      (catch Exception e
        (throw-with-log "Exception reading object '" obj "' for schema '" schema "'." e)))))
;;
;; Encoders
;;
(def json-encoder
  (fn json-encoder-fn [#^Schema schema obj]
    (encode-to schema obj
      (fn [#^Schema schema #^ByteArrayOutputStream stream]
        (.jsonEncoder (EncoderFactory/get) schema stream))
      (fn [#^ByteArrayOutputStream stream]
        (.toString stream "UTF-8")))))

(def binary-encoder
  (fn binary-encoder-fn [#^Schema schema obj]
    (encode-to schema obj
      (fn [#^Schema schema #^ByteArrayOutputStream stream]
        (.binaryEncoder (EncoderFactory/get) stream nil))
      (fn [#^ByteArrayOutputStream stream]
        (.. stream toByteArray)))))

;
; Decoding
;

; Unpack multi method
(defmulti unpack-avro
  (fn [schema obj]
    (.getName #^Schema schema)))

(defmethod unpack-avro :default [schema obj] obj)

(declare unpack-impl json-schema)

;; TODO: Will revisit this with regard to generating keywords.
;;
;; (defn- unpack-record-fields
;;   "Unpack only provided fields from record object."
;;   [#^Schema schema #^GenericData$Record obj opts make-key fields rmap]
;;   (loop [[f & fs] fields m rmap]
;;     (if f
;;       (if (coll? f)
;;         (if-let [#^Schema$Field fd (.getField schema (name (first f)))]
;;           (let [k (.name fd)]
;;             (if (next f)
;;               (recur fs
;;                      (assoc m (make-key k)
;;                             (unpack-record-fields (.schema fd) (.get obj k) opts make-key (rest f) (m k))))
;;               (recur fs (assoc m k (unpack-impl (.schema fd) (.get obj k) opts)))))
;;           (throw-with-log "No field for name '" (first f) "' exists in schema " (json-schema schema)))
;;         (if-let [#^Schema$Field fd (.getField schema (name f))]
;;           (let [k (.name fd)]
;;             (recur fs (assoc m (make-key k) (unpack-impl (.schema fd) (.get obj k) opts))))
;;           (throw-with-log "No field for name '" f "' exists in schema " (json-schema schema))))
;;       m)))


(defn- unpacker-key-maker
  "Looks at the options for unpacking and,
  if :use-keywords is set, then we want to unpack with keywords"
  [unpacking-opts]
  (if (:use-keywords unpacking-opts)
    #(keyword (str %))
    str))

(defn- unpack-all-record-fields
  "Unpack entire record object."
  [#^Schema schema #^GenericData$Record obj opts]
  (let [make-key (unpacker-key-maker opts)]
    (persistent!
     (reduce
      (fn [m #^Schema$Field f]
        (let [k (.name f)]
          (assoc! m (make-key k) (unpack-impl (.schema f) (.get obj k) opts))))
      (transient {}) (.getFields schema)))))


;;
;; Unpacking multimethod
;;
(defmulti unpack-obj
  "Unpacks an object according to its schema type"  
  (fn [#^Schema schema obj opts]
    (.getType schema)))

(defmethod unpack-obj Schema$Type/NULL
  [#^Schema schema obj opts]
  (if obj (throw (Exception. "Nil expected."))))

(defmethod unpack-obj Schema$Type/BOOLEAN
  [#^Schema schema obj opts]
  (boolean obj))

(defmethod unpack-obj Schema$Type/INT
  [#^Schema schema obj opts]
  (int obj))

(defmethod unpack-obj Schema$Type/LONG
  [#^Schema schema obj opts]
  (long obj))

(defmethod unpack-obj Schema$Type/FLOAT
  [#^Schema schema obj opts]
  (float obj))

(defmethod unpack-obj Schema$Type/DOUBLE
  [#^Schema schema obj opts]
  (double obj))

(defmethod unpack-obj Schema$Type/BYTES
  [#^Schema schema obj opts]
  (bytes obj))

(defmethod unpack-obj Schema$Type/FIXED
  [#^Schema schema #^GenericData$Fixed obj opts]
  (.bytes obj))

(defmethod unpack-obj Schema$Type/ENUM
  [#^Schema schema obj opts]
  (str obj))

(defmethod unpack-obj Schema$Type/STRING
  [#^Schema schema obj opts]
  (if (instance? Utf8 obj)    
    (str obj)
    (throw (Exception. (str "Object '" obj "' is not a Utf8.")))))

(defmethod unpack-obj Schema$Type/UNION
  [#^Schema schema obj opts] 
  (loop [schemas (.getTypes schema)]
    (if (empty? schemas)
      (throw-with-log "No union type defined for object '" obj "'.")
      (let [rec (try
                  (unpack-impl (first schemas) obj opts)
                  (catch Exception e :not-matching-untion-type))]
        (if (not= rec :not-matching-untion-type)
          rec
          (recur (next schemas)))))))

(defmethod unpack-obj Schema$Type/ARRAY
  [#^Schema schema obj opts] 
  (let [type-schema (.getElementType schema)]
    (vec (map #(unpack-impl type-schema % opts) obj))))

(defmethod unpack-obj Schema$Type/MAP
  [#^Schema schema obj opts] 
  (let [make-key (unpacker-key-maker opts)
        type-schema (.getValueType schema)]      
    (reduce (fn [m [k v]] (assoc m (make-key k) (unpack-impl type-schema v opts))) {} obj)))

(defmethod unpack-obj Schema$Type/RECORD
  [#^Schema schema #^GenericData$Record obj opts]
  ;;(if (empty? (:fields opts))
  (unpack-all-record-fields schema obj opts))
;;    (unpack-record-fields schema obj opts (unpacker-key-maker opts) (:fields opts) {})))

(defmethod unpack-obj :default
  [#^Schema schema obj opts]
  (throw-with-log "No unpack defined for type '" (.getType schema) "'."))


(defn- unpack-impl
  [#^Schema schema obj opts]
  (unpack-avro schema (unpack-obj schema obj opts)))
     

(defn unpack
  [schema obj & {:keys [decoder fields use-keywords] :or {:fields []}}]
  (let [#^Schema schema   (avro-schema schema)
        decode   (or decoder (fn [_ obj] obj))
        obj      (decode schema obj)]
    (try
      (unpack-impl schema obj {:use-keywords use-keywords :fields fields})
      (catch Exception e
        (throw-with-log "Exception unpacking object '" obj "' for schema '" schema "'." e)))))
  
(defn- decode-from
  [schema obj decoder]
  (let [reader  (GenericDatumReader. schema)
        decoder (decoder schema obj)]
    (.read reader nil decoder)))

(def json-decoder
  (fn json-decoder-fn [#^Schema schema obj]
    (decode-from schema obj
      (fn decode-from-json [#^Schema schema #^String obj]
        (let [is (ByteArrayInputStream. (.getBytes obj "UTF-8"))]
          (.jsonDecoder (DecoderFactory/get) schema obj))))))

(def binary-decoder
  (let [factory (DecoderFactory/defaultFactory)
        decode-from-binary (fn decode-from-binary
                             [#^Schema schema #^bytes obj]
                             (.binaryDecoder factory obj nil))]
    (fn binary-decoder-fn [#^Schema schema obj]
      (decode-from schema obj decode-from-binary))))

;
; Custom avro type methods
;

(defmacro def-unpack-avro
  [type f]
  `(defmethod unpack-avro ~(str type) [s# o#] (~f o#)))

; 
; Avro schema generation
;

(def ^:dynamic named-types nil)

(defn- traverse-schema
  "Traverse types of a schema."
  [schema f]
  (cond 
    (vector? schema)
      (vec (map #(f %) schema))

    (map? schema)
      (case (:type schema)
        "array"   (assoc schema :items (f (:items schema)))
        "map"     (assoc schema :values (f (:values schema)))
        "record"  (assoc schema :fields (vec (map #(assoc % :type (f (:type %))) (:fields schema))))
        schema)
    :else schema))

(defn- flatten-named-types
  "Ensures a named type is only defined once."
  [schema]
  (if-let [name (:name schema)]
    (if (some @named-types [name])
      name
      (let [schema (traverse-schema schema flatten-named-types)]
        (swap! named-types conj name)
        schema))
    (traverse-schema schema flatten-named-types)))
        
(defn avro-schema
  "Convert a simple-avro or json string schema to Avro schema object."
  [schema]
  (cond 
    (instance? Schema schema)
      schema
    (string? schema)
      (Schema/parse #^String schema)
    :else
      (let [schema (binding [named-types (atom #{})]
                     (flatten-named-types schema))]
        (Schema/parse #^String (json/json-str schema)))))

(defn json-schema
  "Print schema to a json string. Provide optional parameter {:pretty bool}
  for pretty printing. Default is false."
  [schema & [opts]]
  (let [pretty (or (:pretty opts) false)]
    (.toString #^Schema (avro-schema schema) pretty)))

