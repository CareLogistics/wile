(ns wile.core-test
  (:require [clojure.test :refer :all]
            [wile.core :refer :all]
            [datomic.api :as api]))

(def sample-attr-tx
  [{:db/id                 #db/id[:db.part/db]
    :db/ident              :example
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def decorate-tx #(conj % {:db/id #db/id[:db.part/tx]
            :example "intercepted"}))

(def log-tx #(swap! %1 conj %2))

(def deny-access (fn [_ _] false))

(defn exclude-tx-inst
  [insts]
  (fn [db datom]
    (not
     (contains? insts (:db/txInstant (api/pull db '[*] (:tx datom)))))))

(deftest test-gumbo
  (let [_               (api/create-database "datomic:mem://wile")
        wile-connection (api/connect "datomic:mem://wile")]

    (testing "spying"
      (let [tx-log (atom [])
            spy-conn (spy-transact wile-connection (executor) (partial log-tx tx-log))]

        @(api/transact spy-conn sample-attr-tx)
        (Thread/sleep 100)              ; allow spy function to be invoked
        (is (first @tx-log))))

    (testing "intercept"
      (let [f22        (intercept-transact wile-connection decorate-tx)
            tx-result @(api/transact f22 sample-attr-tx)
            tx-id      (.tx (first (:tx-data tx-result)))]

        (is (:example (api/pull (api/db f22) '[*] tx-id)))))

    (testing "filter-db"
      (let [deep6 (filter-db wile-connection (executor) deny-access)]

        (is (= {:db/id 0} (api/pull (api/db deep6) '[*] 0)))))

    (testing "fully cooked gumbo"
      (let [e (executor)
            tx-log (atom [])
            tx0-inst (clojure.edn/read-string "#inst \"1970-01-01T00:00:00.000-00:00\"")

            jumbalaya (-> wile-connection
                          (spy-transact e     (partial log-tx tx-log))
                          (intercept-transact  decorate-tx)
                          (filter-db e        (exclude-tx-inst #{tx0-inst})))

            tx-result @(api/transact jumbalaya sample-attr-tx)
            tx-id      (.tx (first (:tx-data tx-result)))]

        @(api/transact jumbalaya sample-attr-tx)
        (Thread/sleep 100)              ; allow spy function to be invoked
        (is (first @tx-log))

        (is (:example (api/pull (api/db jumbalaya) '[*] tx-id)))

        (is (= 1 (count (api/q '[:find ?a :where [_ :db.install/attribute ?a]] (api/db jumbalaya)))))))))
