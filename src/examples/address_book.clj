(ns examples.address-book
  (:use (simple-avro schema core)))

; Schema

(defavro-enum Country
  "USA" "Germany" "France" ; ...
  )

(defavro-enum State
  "AL" "AK" "AS" "AZ" "AR" "CA" "CO" ; ...
  )

(defavro-record Address
  :street  avro-string
  :city    avro-string
  :state   State
  :zip     avro-int
  :things (avro-array (avro-union avro-string avro-int))
  :country Country)

(defavro-record Person
  :first   avro-string
  :last    avro-string
  :address Address
  :email   avro-string
  :phone   (avro-union avro-string avro-null))

(defavro-record Company
  :name    avro-string
  :address Address
  :contact Person)

(def Contact
  (avro-union Person Company))

(def AddressBook
  (avro-array Contact))


;; Sample records

(def address-book
  [{:first  "Mike"
    :last    "Foster"
    :address {:street  "South Park Str. 14"
              :city    "Wasco"
              :state   "CA"
              :zip     95171
              :things [5 "hello"]
              :country "USA"}
    :email   "mike@home.com"
    :phone   nil}])

(def address-book-string
  [{"first"  "Mike"
   "last"    "Foster"
   "address" {"street"  "South Park Str. 14"
              "city"    "Wasco"
              "state"   "CA"
              "zip"     95171
              "things" [5 "hello"]
              "country" "USA"}
   "email"   "mike@home.com"
   "phone"   nil}])

(def address-book-mixed
  [{"first"  "Mike"
    :last    "Foster"
    "address" {"street"  "South Park Str. 14"
               :city    "Wasco"
               "state"   "CA"
               "zip"     95171
               :things [5 "hello"]
               "country" "USA"}
    "email"   "mike@home.com"
    :phone   nil}])


;; Serializations

;; keywords version
(let [packed-address-book (pack AddressBook address-book)
      unpacked-address-book (unpack AddressBook packed-address-book)]
  (assert (= address-book unpacked-address-book)))

;; string version
(let [packed-address-book-string   (pack AddressBook address-book-string)
      unpacked-address-book-string
      (unpack AddressBook packed-address-book-string :str-key true)]
  (assert (= address-book-string unpacked-address-book-string)))

;; mixed keywords and strings version
(let [packed-address-book-mixed   (pack AddressBook address-book-mixed)
      unpacked-address-book-mixed (unpack AddressBook
                                          packed-address-book-mixed)]
  (assert (= address-book unpacked-address-book-mixed)))

(let [packed-address-book-mixed   (pack AddressBook address-book-mixed)
      unpacked-address-book-mixed (unpack AddressBook
                                          packed-address-book-mixed
                                          :str-key true)]
  (assert (= address-book-string unpacked-address-book-mixed)))
