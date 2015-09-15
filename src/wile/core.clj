(ns wile.core
  (:require [clojure.java.io :as io]
            [datomic
             [api :as api]
             [promise :refer [settable-future]]])

  (:import [datomic
            Connection
            Database$Predicate]
           [java.util.concurrent Executors]))

(defn executor [] (Executors/newCachedThreadPool))

(defn notify
  "Invokes a function for it's side effects & ignore it's results."
  [f & more]
  {:pre [(ifn? f)]}
  (io!
   (try (apply f more)
        (catch Throwable _ nil)
        (finally nil))))

(defn notify-all
  "Invokes a coll of fns for their side effects & ignore it's results. Immediately
  returns a future."
  [fns & more]
  {:pre [(every? ifn? fns)]}
  (future (doseq [f fns] (apply notify f more))))

(defn lmap
  "Maps the result of a ListenableFuture."
  ([e f l]
   (let [r (settable-future)]
     (.addListener l #(deliver r (f @l)) e)
     r))
  ([f l] (lmap (executor) f l)))

(defn intercept-transact
  "Transactions to connection will be intercepted by intercepters.  Intercepters
  must return a valid transaction data structure."
  [^Connection cnx & intercepters]

  {:pre [(every? ifn? intercepters)]}

  (let [pre-tx (reduce comp intercepters)]
    (reify
      Connection
      (transact      [_ tx-data] (.transact      cnx (pre-tx tx-data)))
      (transactAsync [_ tx-data] (.transactAsync cnx (pre-tx tx-data)))

      ;; "boiler-plate that I should wrap in a macro rather than
      ;; copy-pasting"
      (db [_] (.db cnx))
      (sync [_] (.sync cnx))
      (sync [_ t] (.sync cnx t))
      (syncIndex [_ t] (.syncIndex cnx t))
      (syncSchema [_ t] (.syncSchema cnx t))
      (syncExcise [_ t] (.syncExcise cnx t))
      (log [_] (.log cnx))
      (requestIndex [_] (.requestIndex cnx))
      (txReportQueue [_] (.txReportQueue cnx))
      (removeTxReportQueue [_] (.removeTxReportQueue cnx))
      (gcStorage [_ older-than] (.gcStorage cnx older-than))
      (release [_] (.release cnx)))))

(defn- notify-spies
  [spies tx-data tx-result]
  (do
    (notify-all spies
                (with-meta tx-data (merge (meta tx-data)
                                          {:tx-result tx-result}))),
    tx-result))

(defn spy-transact
  "Notifies spies when a transaction is completed. Spy function takes a single
  argument, the original tx-data with the transact result attached as meta."
  [cnx executor & spies]
  {:pre [(every? ifn? spies)]}

  (let [spy-tx (fn [f tx-data]
                 (lmap executor
                       (partial notify-spies spies tx-data)
                       (f cnx tx-data)))]

    (reify
      Connection
      (transact      [_ tx-data] (spy-tx api/transact       tx-data))
      (transactAsync [_ tx-data] (spy-tx api/transact-async tx-data))

      ;; "boiler-plate that I should wrap in a macro rather than
      ;; copy-pasting"
      (db [_] (.db cnx))
      (sync [_] (.sync cnx))
      (sync [_ t] (.sync cnx t))
      (syncIndex [_ t] (.syncIndex cnx t))
      (syncSchema [_ t] (.syncSchema cnx t))
      (syncExcise [_ t] (.syncExcise cnx t))
      (log [_] (.log cnx))
      (requestIndex [_] (.requestIndex cnx))
      (txReportQueue [_] (.txReportQueue cnx))
      (removeTxReportQueue [_] (.removeTxReportQueue cnx))
      (gcStorage [_ older-than] (.gcStorage cnx older-than))
      (release [_] (.release cnx)))))

(defn filter-db
  "Applies db-filters to all dbs returned by connection."
  [cnx executor & db-filters]

  (let [filter-db #(reduce api/filter % db-filters)
        filter-f-db (partial lmap executor filter-db)
        filter-tx-dbs (partial lmap
                               executor
                               #(-> %
                                    (update-in [:db-before] filter-db)
                                    (update-in [:db-after]  filter-db)))]

    (reify
      Connection
      (db         [_]   (filter-db   (.db cnx)))
      (sync       [_]   (filter-f-db (.sync cnx)))
      (sync       [_ t] (filter-f-db (.sync cnx t)))
      (syncIndex  [_ t] (filter-f-db (.syncIndex cnx t)))
      (syncSchema [_ t] (filter-f-db (.syncSchema cnx t)))
      (syncExcise [_ t] (filter-f-db (.syncExcise cnx t)))
      (transact      [_ tx-data] (filter-tx-dbs (.transact cnx tx-data)))
      (transactAsync [_ tx-data] (filter-tx-dbs (.transactAsync cnx tx-data)))

      ;; "boiler-plate that I should wrap in a macro rather than
      ;; copy-pasting"
      (log [_] (.log cnx))
      (requestIndex [_] (.requestIndex cnx))
      (txReportQueue [_] (.txReportQueue cnx))
      (removeTxReportQueue [_] (.removeTxReportQueue cnx))
      (gcStorage [_ older-than] (.gcStorage cnx older-than))
      (release [_] (.release cnx)))))

(defn datomic-wile
  [cnx executor & {:as options
                   :keys [intercepters spies db-filters]
                   :or {intercepters ()
                        spies ()
                        db-filters ()}}]
  (cond-> cnx
    (seq spies)        (#(apply spy-transact       % executor spies))
    (seq db-filters)   (#(apply filter-db          % executor db-filters))
    (seq intercepters) (#(apply intercept-transact %          intercepters))))

(defn- passthru
  [cnx]
  (reify
    Connection
    ;; "boiler-plate that I should wrap in a macro rather than
    ;; copy-pasting"
    (transact [_ tx-data] (.transact cnx tx-data))
    (transactAsync [_ tx-data] (.transactAsync cnx tx-data))
    (db [_] (.db cnx))
    (sync [_] (.sync cnx))
    (sync [_ t] (.sync cnx t))
    (syncIndex [_ t] (.syncIndex cnx t))
    (syncSchema [_ t] (.syncSchema cnx t))
    (syncExcise [_ t] (.syncExcise cnx t))
    (log [_] (.log cnx))
    (requestIndex [_] (.requestIndex cnx))
    (txReportQueue [_] (.txReportQueue cnx))
    (removeTxReportQueue [_] (.removeTxReportQueue cnx))
    (gcStorage [_ older-than] (.gcStorage cnx older-than))
    (release [_] (.release cnx))))
