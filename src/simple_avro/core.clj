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
  nil) ;; packing anything to NULL (even valid object) should give NULL

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
      (let [#^Schema schema (first schemas)]
        (if (= (.getType schema) Schema$Type/NULL)
          ;; For example we have a schema with [NULL, Real-type]. When trying to
          ;; determine data type to pack data into union - NULL always works
          ;; because it is present in schemas and we can pack anything to NULL..
          ;; So need to skip this step if the passed data is NOT NULL.
          (when-not (nil? obj)
            (recur (next schemas)))
          (let [rec (try
                      (pack schema obj)
                      (catch Exception e :not-matching-union-type))]
            (if (= rec :not-matching-union-type)
              (recur (next schemas))
              rec)))))))

(defmethod pack-obj Schema$Type/ARRAY
  [#^Schema schema obj]
  (let [type-schema (.getElementType schema)
        array       (GenericData$Array. (count obj) schema)]
    (doseq [e obj] (.add array (pack type-schema e)))
    array))

(defmethod pack-obj Schema$Type/MAP
  [#^Schema schema obj]
  (let [type-schema (.getValueType schema)]
    (persistent! (reduce (fn [m [k v]] (assoc! m (name k) (pack type-schema v)))
                         (transient {})
                         obj))))

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
        encode (or encoder (fn default-encoder [_ obj] obj))
        obj    (avro-pack obj)]
    (try
      (encode schema (pack-obj schema obj))
      (catch Exception e
        (throw-with-log "Exception reading object '" obj
                        "' for schema '" schema "'." e)))))

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

;;
;; Unpacking multimethod
;;
(defmulti unpack-obj
  "Unpacks an object according to its schema type"
  (fn [#^Schema schema obj make-key]
    (.getType schema)))

(defmethod unpack-obj Schema$Type/NULL
  [#^Schema schema obj make-key]
  (if obj (throw (Exception. "Nil expected."))))

(defmethod unpack-obj Schema$Type/BOOLEAN
  [#^Schema schema obj make-key]
  (boolean obj))

(defmethod unpack-obj Schema$Type/INT
  [#^Schema schema obj make-key]
  (int obj))

(defmethod unpack-obj Schema$Type/LONG
  [#^Schema schema obj make-key]
  (long obj))

(defmethod unpack-obj Schema$Type/FLOAT
  [#^Schema schema obj make-key]
  (float obj))

(defmethod unpack-obj Schema$Type/DOUBLE
  [#^Schema schema obj make-key]
  (double obj))

(defmethod unpack-obj Schema$Type/BYTES
  [#^Schema schema obj make-key]
  (bytes obj))

(defmethod unpack-obj Schema$Type/FIXED
  [#^Schema schema #^GenericData$Fixed obj make-key]
  (.bytes obj))

(defmethod unpack-obj Schema$Type/ENUM
  [#^Schema schema obj make-key]
  (str obj))

(defmethod unpack-obj Schema$Type/STRING
  [#^Schema schema obj make-key]
  (if (instance? Utf8 obj)
    (str obj)
    (throw (Exception. (str "Object '" obj "' is not a Utf8.")))))

;; TODO: Maybe there is a similar bug to one observed on decoding objects?
(defmethod unpack-obj Schema$Type/UNION
  [#^Schema schema obj make-key]
  (loop [schemas (.getTypes schema)]
    (if (empty? schemas)
      (throw-with-log "No union type defined for object '" obj "'.")
      (let [rec (try
                  (unpack-impl (first schemas) obj make-key)
                  (catch Exception e :not-matching-union-type))]
        (if (not= rec :not-matching-union-type)
          rec
          (recur (next schemas)))))))

(defmethod unpack-obj Schema$Type/ARRAY
  [#^Schema schema obj make-key]
  (let [type-schema (.getElementType schema)]
    (vec (map #(unpack-impl type-schema % make-key) obj))))

(defmethod unpack-obj Schema$Type/MAP
  [#^Schema schema obj make-key]
  (let [type-schema (.getValueType schema)]
    (persistent!
      (reduce
        (fn [m [k v]]
          (assoc! m (make-key k) (unpack-impl type-schema v make-key)))
        (transient {}) obj))))

(defmethod unpack-obj Schema$Type/RECORD
  [#^Schema schema #^GenericData$Record obj make-key]
  (persistent!
    (reduce
      (fn [m #^Schema$Field f]
        (let [k (.name f)]
          (assoc! m (make-key k)
                  (unpack-impl (.schema f) (.get obj k) make-key))))
      (transient {}) (.getFields schema))))

(defmethod unpack-obj :default
  [#^Schema schema obj make-key]
  (throw-with-log "No unpack defined for type '" (.getType schema) "'."))

(defn- unpack-impl
  [#^Schema schema obj key-maker]
  (unpack-avro schema (unpack-obj schema obj key-maker)))

(defn- unpack-particular-field-path
  [#^Schema schema #^GenericData$Record obj make-key field-path]
  (let [[field & rest-fields] field-path]
    (if-not field
      (unpack-impl schema obj make-key)
      (let [field-name (name field)]
        (if-let [#^Schema$Field fd (.getField schema field-name)]
          `{~(make-key field-name)
            ~(unpack-particular-field-path
               (.schema fd) (.get obj (.name fd)) make-key rest-fields)}
          (throw-with-log "No field for name '" field-name
                          "' exists in schema " (json-schema schema)))))))

(defn- deep-merge
  "like merge... but more recursive"
  [v1 v2]
  (if (and (map? v1) (map? v2))
    (merge-with deep-merge v1 v2)
    v2))

;; This is probably un-optimal, but it made the code a lot simpler to manage
(defn- unpack-particular-field-paths
  "We assume that field-paths here have already been pathified. We also assume
   the paths are unique, otherwise, the last one processed will win."
  [#^Schema schema #^GenericData$Record rec make-key field-paths]
  (apply
    ;; deep-merge will merge sub maps at corresponding locations
    (partial merge-with deep-merge)
    (map (partial unpack-particular-field-path schema rec make-key)
         field-paths)))

(defn- unpacker-key-maker
  "If we should make string keys, we will, otherwise, we'll make keyword keys"
  [should-make-string-keys?]
  (if should-make-string-keys?
    str
    #(keyword (str %))))

(defn- record-schema?
  [#^Schema schema]
  (= (.getType schema) Schema$Type/RECORD))

(defn- pathify [key-or-keys]
  (if (coll? key-or-keys)
    key-or-keys
    [key-or-keys]))

(defn unpack
  "Unpack a particular Avro Object with a given schema.
   Optional keyword arguements:
  - :fields [:field1 [:field2 :innerfield] :field3]
    If the schema is of an Avro Record type, and the object is a record,
    select only the specified fields will be extracted.
    Moreover, if a 'field-specification' is a collection of keys
    as demonstrated in the second element above, this is treated as a property
    path (e.g. field2.innerfield)
  - :use-keywords
    If specified as false, the result map will be generated with strings as keys
    for property names as opposed to keywords
  - :decoder
    a function to decode an un-avroed property, perhaps into something that
    can be manipulated in code further down."
  [schema obj
   & {:keys [decoder fields str-key]
      :or {:fields [], :decoder false, :str-key false}}]
  (let [#^Schema schema   (avro-schema schema)
        decode   (or decoder (fn [_ obj] obj))
        obj      (decode schema obj)
        key-maker (unpacker-key-maker str-key)]
    (try
      (if (and fields (record-schema? schema))
        ;; pathify fields for uniformity
        (unpack-particular-field-paths schema obj key-maker (map pathify
                                                                 fields))
        (unpack-impl schema obj key-maker))
      (catch Exception e
        (throw-with-log "Exception unpacking object '" obj
                        "' for schema '" schema "'." e)))))

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
      (vec (map f schema))

    (map? schema)
      (case (:type schema)
        "array" (update-in schema [:items] f)
        "map"  (update-in schema [:values] f)
        "record"  (assoc schema :fields (vec (map #(update-in % [:type] f)
                                                  (:fields schema))))
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
