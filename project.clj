(defproject wile "0.0.0-2"
  :description
  "A few simple wrapper functions for datomic."

  :url "http://github.com/CareLogistics"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.7.0"
                  :scope "provided"]
                 [com.datomic/datomic-free "0.9.5206"
                  :scope "provided"
                  :exclusions [joda-time]]
                 [clj-time "0.11.0"
                  :scope "provided"]])
