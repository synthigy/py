;; Build + deploy the "Synthigy Music" ERD, and export the deployable JSON
;; artifact next to this file. Run from the Synthigy server REPL (core nREPL)
;; once — it creates the Music Album/Track/Artist/Genre tables and writes
;; synthigy-music@0.1.0.json.
;;
;;   (load-file "sdk/py/examples/datastar-music/datasets/build.clj")
;;
;; NOTE: entity names are globally unique across ALL datasets, so they carry a
;; "Music " prefix (bare "Genre" already exists elsewhere and collides). This is
;; a FRESH-DB reproducer only: on a DB that already has the modeler-managed
;; "Music Demo" dataset (the live working one, same entity names), do NOT run
;; this — it will conflict. Destroy first if re-creating:
;;   (require '[synthigy.dataset :as ds]) (ds/destroy! {:name "Synthigy Music"})

(require '[synthigy.dataset.core :as dcore]
         '[synthigy.dataset.id :as id]
         '[synthigy.dataset :as ds]
         '[synthigy.transit :as transit])

(defn- attr [seq name type & [constraint]]
  (dcore/map->ERDEntityAttribute
   {:euuid (random-uuid) :xid (id/generate-xid) :seq seq :name name
    :constraint (or constraint "optional") :type type :configuration nil :active true}))

(defn- entity [name x y attrs & [unique]]
  (dcore/map->ERDEntity
   {:euuid (random-uuid) :xid (id/generate-xid)
    :position {:x x :y y} :width 160 :height 160
    :name name :attributes attrs :type "STRONG"
    :configuration (when unique {:constraints {:unique unique}})
    :clone nil :original nil :active true :claimed-by nil}))

(defn- relation [from to from-label to-label card]
  (dcore/map->ERDRelation
   {:euuid (random-uuid) :xid (id/generate-xid)
    :from from :to to :from-label from-label :to-label to-label
    :cardinality card :path nil :configuration nil :active true :claimed-by nil}))

(let [alb-title (attr 0 "Title" "string" "mandatory")
      ;; Title is mandatory but NOT unique — different artists ship albums with
      ;; the same name ("Greatest Hits", "1", "Discovery"). Album identity is a
      ;; deterministic xid from artist+title (seed.py / prep_spotify.bb), so
      ;; re-seeds upsert by id without a false unique constraint.
      album     (entity "Music Album" -220 0
                        [alb-title (attr 1 "Blurb" "string")
                         (attr 2 "Plays" "int") (attr 3 "Link" "string")
                         (attr 4 "Cover" "string")])
      track     (entity "Music Track" 120 -140
                        [(attr 0 "Title" "string" "mandatory")
                         (attr 1 "Released On" "timestamp")
                         (attr 2 "Explicit" "boolean")
                         (attr 3 "Length" "string")])
      a-name    (attr 0 "Name" "string" "mandatory")
      artist    (entity "Music Artist" 120 60 [a-name (attr 1 "Country" "string")] [[(:xid a-name)]])
      g-label   (attr 0 "Label" "string" "mandatory")
      genre     (entity "Music Genre" 120 240 [g-label] [[(:xid g-label)]])
      model (dcore/map->ERDModel
             {:id-key nil
              :entities (into {} (map (juxt :xid identity)) [album track artist genre])
              :relations (into {} (map (juxt :xid identity))
                               [(relation (:xid album) (:xid track)  "album"  "tracks"  "o2m")
                                (relation (:xid album) (:xid artist) "albums" "artists" "m2m")
                                (relation (:xid album) (:xid genre)  "albums" "genres"  "m2m")])
              :configuration nil :clones nil :version "1.0.0"})
      version {:euuid (random-uuid) :name "0.1.0" :xid (id/generate-xid)
               :dataset {:euuid (random-uuid) :name "Synthigy Music" :xid (id/generate-xid)}
               :model model}]
  (ds/deploy! version)
  (spit "sdk/py/examples/datastar-music/datasets/synthigy-music@0.1.0.json"
        (transit/->transit version))
  (println "Deployed + exported. Album xid:" (:xid album)))
