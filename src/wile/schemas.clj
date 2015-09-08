(ns wile.schemas
  (:require [datomic.api :as d]))

(defn- read-schema
  [s]
  (clojure.edn/read-string
   {:readers *data-readers*}
   (slurp (io/resource (format "schema/%s.edn" (name s))))))
