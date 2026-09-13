#!/usr/bin/env bb
;; Transform the Kaggle "Spotify Global Music Dataset (2009–2025)" CSV into
;; Synthigy import JSON for the Music demo — the music mirror of movies'
;; prep_movielens.bb.
;;
;; Like movies, identity is a DETERMINISTIC xid, NOT a unique business column.
;; Albums/tracks key off the dataset's own real Spotify ids (album_id/track_id)
;; — no title-collision guesswork needed. Artists/genres key off name/label.
;; Same input → same ids → re-running is idempotent (upsert, never duplicates).
;; The xid algorithm is a faithful port of synthigy.dataset.id/uuid->nanoid
;; (UUID v3 over the key, then Base58) — verified byte-for-byte against the
;; server.
;;
;; Dataset: "Spotify Global Music Dataset (2009-2025)"
;; (wardabilal/spotify-global-music-dataset-20092025), file track_data_final.csv
;; (~8.8k tracks / ~5.3k albums / ~2.5k artists / ~420 genres). Columns used:
;;   track_id · track_name · track_duration_ms · explicit
;;   artist_name · artist_genres
;;   album_id · album_name · album_release_date · album_type · album_total_tracks
;;
;; Usage:  bb prep_spotify.bb <track_data_final.csv> [out-dir] [--top N|all]
;;   out-dir defaults to this script's dir; --top caps albums (default 300),
;;   ranked by total track_popularity so you get recognizable records first.
;;   Pass --top all for the full dataset (~5.3k albums, ~8.8k tracks).
;;
;; Produces (consumed directly by seed.py — records carry xids, relations by xid):
;;   artists.json  [{xid, name}]
;;   genres.json   [{xid, label}]
;;   albums.json   [{xid, title, blurb, plays, link, artists:[{xid}], genres:[{xid}],
;;                   tracks:[{xid, title, release_on, length, explicit}]}]

(require '[clojure.string :as str]
         '[clojure.java.io :as io]
         '[cheshire.core :as json])
(import '[java.io BufferedReader] '[java.nio ByteBuffer] '[java.util UUID])

;; ---------- Deterministic xid (port of id/uuid->nanoid) ------------------

(def ^:private base58 "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

(defn- uuid->nanoid [^UUID u]
  (let [buf (doto (ByteBuffer/allocate 16)
              (.putLong (.getMostSignificantBits u))
              (.putLong (.getLeastSignificantBits u)))
        sb  (StringBuilder.)]
    (loop [n (BigInteger. 1 (.array buf))]
      (when (pos? (.signum n))
        (let [qr (.divideAndRemainder n (BigInteger/valueOf 58))]
          (.append sb (.charAt base58 (.intValue (aget qr 1))))
          (recur (aget qr 0)))))
    (let [s (.toString sb) pad (- 22 (count s))]
      (str (when (pos? pad) (apply str (repeat pad \1))) (str/reverse s)))))

(defn- xid [kind & parts]
  (uuid->nanoid (UUID/nameUUIDFromBytes
                 (.getBytes (str "synthigy-music/" kind "/" (str/join "/" parts)) "UTF-8"))))

;; ---------- CSV (quoted fields, embedded commas) ------------------------

(defn- parse-row [^String line]
  (let [n (count line) sb (StringBuilder.)]
    (loop [i 0 in-q? false fields []]
      (if (= i n)
        (conj fields (.toString sb))
        (let [c (.charAt line i)]
          (cond
            (and (= c \") (not in-q?)) (recur (inc i) true fields)
            (and (= c \") in-q?)
            (if (and (< (inc i) n) (= (.charAt line (inc i)) \"))
              (do (.append sb \") (recur (+ i 2) true fields))
              (recur (inc i) false fields))
            (and (= c \,) (not in-q?))
            (let [s (.toString sb)] (.setLength sb 0)
              (recur (inc i) false (conj fields s)))
            :else (do (.append sb c) (recur (inc i) in-q? fields))))))))

(defn- read-csv [path]
  (with-open [^BufferedReader r (io/reader path)]
    (let [header (mapv keyword (parse-row (.readLine r)))]
      (loop [out (transient [])]
        (if-some [line (.readLine r)]
          (recur (conj! out (zipmap header (parse-row line))))
          (persistent! out))))))

;; ---------- Field normalization -----------------------------------------

(defn- ->int [s] (try (Long/parseLong (str/trim (or s "0"))) (catch Exception _ 0)))

(defn- ms->len [ms]
  (let [sec (quot (->int ms) 1000)]
    (format "%d:%02d" (quot sec 60) (rem sec 60))))

;; album_release_date is usually full ISO ("2024-04-19") but sometimes a bare
;; year ("1967") or year-month ("1967-09") — the server rejects anything but
;; full ISO-8601, so pad to the 1st.
(defn- normalize-date [s]
  (let [s (str/trim (or s ""))]
    (cond
      (re-matches #"\d{4}-\d{2}-\d{2}" s) s
      (re-matches #"\d{4}-\d{2}" s) (str s "-01")
      (re-matches #"\d{4}" s) (str s "-01-01")
      :else nil)))

;; artist_genres is a Python list-literal string: "['pop']", "['a', 'b']", "[]".
(defn- parse-genres [s]
  (let [inner (-> (or s "") str/trim (str/replace #"^\[" "") (str/replace #"\]$" ""))]
    (if (str/blank? inner)
      []
      (->> (str/split inner #",")
           (map #(-> % str/trim
                    (str/replace #"^['\"]" "")
                    (str/replace #"['\"]$" "")))
           (remove str/blank?)))))

;; ---------- Build -------------------------------------------------------

(defn build [rows top]
  ;; Real album_id groups rows precisely — no title-collision heuristics needed.
  (let [groups (->> rows
                    (remove #(or (str/blank? (:album_id %)) (str/blank? (:track_id %))))
                    (group-by :album_id))
        albums
        (for [[album-id rs] groups
              :let [head (first rs)
                    axid (xid "album" album-id)]]
          {:xid axid
           :title (:album_name head)
           ;; Just the type — track count is already shown as its own badge on
           ;; the row/detail card, so repeating "N tracks" here would be noise.
           :blurb (str/capitalize (or (:album_type head) ""))
           :link (str "https://open.spotify.com/album/" album-id)
           :plays (reduce + (map #(->int (:track_popularity %)) rs))
           :artists (->> rs (map :artist_name) (remove str/blank?) distinct sort
                         (mapv (fn [n] {:xid (xid "artist" n) :name n})))
           :genres  (->> rs (mapcat #(parse-genres (:artist_genres %))) distinct sort
                         (mapv (fn [g] {:xid (xid "genre" g) :label g})))
           :tracks  (->> rs
                         (group-by :track_id)          ; guard against dupe rows
                         (map (fn [[tid trs]]
                                (let [r (first trs)]
                                  {:xid (xid "track" tid) :title (:track_name r)
                                   :release_on (normalize-date (:album_release_date r))
                                   :length (ms->len (:track_duration_ms r))
                                   :explicit (= "True" (:explicit r))})))
                         (sort-by :title) vec)})
        ranked (->> albums (sort-by :plays >))]
    (if (= top :all) (vec ranked) (vec (take top ranked)))))

;; ---------- Main --------------------------------------------------------

;; Parse "--top" (and its value) out of the arg list FIRST, wherever it sits —
;; then whatever's left, positionally, is [csv out-dir?]. Doing this by
;; destructuring position alone (old approach) silently mis-parsed
;; `csv --top all` (no out-dir) as out-dir="--top", swallowing the real value.
(let [args (vec *command-line-args*)
      top-idx (.indexOf args "--top")
      top-arg (when (>= top-idx 0) (get args (inc top-idx)))
      positional (vec (keep-indexed
                        (fn [i a] (when (or (neg? top-idx)
                                            (and (not= i top-idx) (not= i (inc top-idx))))
                                    a))
                        args))
      csv (first positional)
      out (or (second positional) (str (.getParent (io/file *file*))))
      top (cond (nil? top-arg) 300
                (= "all" top-arg) :all
                :else (->int top-arg))]
  (when-not csv
    (println "usage: bb prep_spotify.bb <track_data_final.csv> [out-dir] [--top N|all]")
    (System/exit 1))
  (println "reading" csv "…")
  (let [albums (build (read-csv csv) top)
        ;; dedupe reference tables by xid across the selected albums
        artists (->> albums (mapcat :artists) distinct
                     (group-by :xid) vals (map first) (sort-by :name) vec)
        genres  (->> albums (mapcat :genres) distinct
                     (group-by :xid) vals (map first) (sort-by :label) vec)
        albums* (mapv (fn [a]
                        {:xid (:xid a) :title (:title a) :blurb (:blurb a)
                         :plays (:plays a) :link (:link a)
                         :artists (mapv #(select-keys % [:xid]) (:artists a))
                         :genres  (mapv #(select-keys % [:xid]) (:genres a))
                         :tracks  (:tracks a)})
                      albums)
        total-tracks (reduce + (map (comp count :tracks) albums*))
        spit* (fn [f data] (spit (str out "/" f) (json/generate-string data)))]
    (spit* "artists.json" artists)
    (spit* "genres.json" genres)
    (spit* "albums.json" albums*)
    (println (format "wrote %d albums, %d tracks, %d artists, %d genres (linked by xid)"
                     (count albums*) total-tracks (count artists) (count genres)))))
