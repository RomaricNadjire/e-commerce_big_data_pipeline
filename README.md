# E-Commerce Data Engineering Platform

Plateforme pédagogique de Data Engineering construite autour d'un cas e-commerce. Le projet met en œuvre l'ingestion de données batch et temps réel, le stockage distribué, le traitement des données, l'orchestration des pipelines et la préparation d'indicateurs analytiques.

> **État du projet :** le pipeline batch avec HDFS, Spark et Airflow est développé. L'API FastAPI communique avec le endpoint HTTP de NiFi. Le routage temps réel complet vers HDFS et Kafka, puis la consommation et la réconciliation des événements, reste à finaliser et à valider.

## Sommaire

- [1. Objectifs](#1-objectifs)
- [2. Technologies utilisées](#2-technologies-utilisées)
- [3. Architecture](#3-architecture)
- [4. Organisation du projet](#4-organisation-du-projet)
- [5. Installation et démarrage](#5-installation-et-démarrage)
- [6. Génération et stockage des données](#6-génération-et-stockage-des-données)
- [7. Pipeline batch : Airflow et Spark](#7-pipeline-batch--airflow-et-spark)
- [8. API et ingestion temps réel](#8-api-et-ingestion-temps-réel)
- [9. Kafka](#9-kafka)
- [10. PostgreSQL](#10-postgresql)
- [11. Visualisation avec Metabase](#11-visualisation-avec-metabase)
- [12. Commandes de diagnostic](#12-commandes-de-diagnostic)
- [13. Points d'attention](#13-points-dattention)
- [14. Améliorations prévues](#14-améliorations-prévues)

## 1. Objectifs

Le projet simule une plateforme e-commerce produisant des données de référence et des transactions. Il permet de pratiquer :

- l'ingestion de fichiers et d'événements ;
- le stockage distribué avec HDFS ;
- le nettoyage, la validation et l'enrichissement avec Spark ;
- l'organisation des données en couches Raw, Silver et Gold ;
- l'orchestration des traitements avec Airflow ;
- le transport des événements avec Kafka ;
- l'exposition d'une API avec FastAPI ;
- le stockage opérationnel et analytique avec PostgreSQL.

L'objectif est également de comprendre les choix d'architecture : quand utiliser le batch ou le streaming, pourquoi séparer les données brutes des données transformées, comment gérer les erreurs et comment éviter de retraiter plusieurs fois les mêmes transactions.

## 2. Technologies utilisées

| Technologie            | Rôle                                                              |
| ---------------------- | ----------------------------------------------------------------- |
| Docker Compose         | Démarrage et réseau des services                                  |
| Apache NiFi 2.11       | Ingestion, routage et transfert des données                       |
| Apache Kafka 4.1       | Transport des événements en flux                                  |
| Apache Hadoop HDFS 3.5 | Stockage distribué des fichiers                                   |
| Apache Spark 4.2       | Traitements distribués et agrégations                             |
| Apache Airflow 3.3     | Orchestration et suivi des traitements batch                      |
| PostgreSQL 16          | Référentiels, transactions opérationnelles et données analytiques |
| FastAPI                | API HTTP pour la création de transactions                         |
| Python                 | Génération de données, API, consommateurs et traitements          |
| Pydantic               | Validation des données échangées par l'API                        |

Les versions indiquées correspondent à l'environnement de développement du projet.

## 3. Architecture

### 3.1. Vue générale

```text
                    SOURCES DE DONNÉES
                ┌────────────┴────────────┐
                │                         │
           Fichiers batch           Application de vente
                │                       Streamlit
                │                          │
                │                       FastAPI
                │                          │
                ▼                          ▼
               NiFi ◄────────────── Endpoint HTTP NiFi
                │                          │
                ▼                   ┌──────┴──────┐
             HDFS Raw                │             │
                │                    ▼             ▼
                │                 HDFS Raw       Kafka
                │                                  │
                │                                  ▼
                │                              Consumer
                │                                  │
                │                                  ▼
                │                             PostgreSQL
                ▼                          (opérationnel)
              Airflow
                │
                ▼
               Spark
                │
          ┌─────┴─────┐
          ▼           ▼
        Silver       Gold
          │           │
          └─────┬─────┘
                ▼
           PostgreSQL
        tables analytiques
                │
                ▼
            Metabase
        dashboards BI
```

Ce schéma représente l'architecture cible. Le chemin FastAPI → NiFi HTTP a été testé, mais chaque destination doit encore être vérifiée séparément. Ne considère pas le flux temps réel comme terminé tant que les écritures HDFS et Kafka, ainsi que le consommateur, n'ont pas été testés de bout en bout.

### 3.2. Les couches de données

- **Raw** : données conservées au plus près de leur forme d'arrivée. Cette couche sert de référence pour l'audit et les retraitements.
- **Silver** : données typées, nettoyées, validées et enrichies. Les transactions sont notamment contrôlées par rapport aux clients et produits disponibles dans Silver.
- **Gold** : données agrégées pour les analyses, par exemple les ventes, les volumes de transactions et les annulations.
- **Quarantine** : enregistrements écartés du flux analytique principal, soit parce qu'ils sont invalides, soit parce qu'une référence n'est pas encore disponible.

PostgreSQL opérationnel et les couches analytiques répondent à des besoins différents. Une transaction présente dans PostgreSQL n'est pas nécessairement déjà validée dans Silver ou intégrée dans Gold.

### 3.3. Pourquoi conserver Raw pour les événements temps réel ?

Les données batch et temps réel doivent pouvoir suivre des règles de transformation communes. Conserver les événements bruts dans HDFS permet de les auditer et de les retraiter indépendamment de l'état du consommateur Kafka.

Kafka transporte les événements et permet leur consommation. HDFS Raw conserve les données pour les traitements analytiques. Ces rôles sont complémentaires.

## 4. Organisation du projet

```text
.
├── airflow/
│   ├── dags/ecommerce_pipeline.py
│   ├── dags-backups/
│   └── logs/
├── api/
│   ├── app/
│   │   ├── main.py
│   │   ├── schemas.py
│   │   └── services/transaction_service.py
│   ├── Dockerfile
│   └── requirements.txt
├── data/
│   ├── generated/
│   └── sample/
├── db/load_reference_data.py
├── docs/
│   ├── architecture.md
│   └── data-model.md
├── hadoop/config/
├── kafka/
│   ├── consumer/
│   ├── create_topics.sh
│   └── scripts/
├── nifi/
│   ├── drivers/
│   ├── extensions/
│   └── flows/
├── postgres/init/
│   ├── 001_schema.sql
│   └── 002_analytics.sql
├── scripts/generate_data.py
├── spark/jobs/
├── docker-compose.yml
└── README.md
```

Les dossiers `airflow/logs`, `__pycache__` et les bases locales Airflow contiennent des artefacts de développement. Ils ne sont généralement pas à versionner. Ne versionne jamais de secrets ni de mots de passe.

## 5. Installation et démarrage

### 5.1. Prérequis

- Docker Engine ;
- Docker Compose ;
- Git ;
- Python pour les scripts exécutés localement.

Vérifier les installations :

```bash
docker --version
docker compose version
python3 --version
```

### 5.2. Démarrer les services

Depuis la racine du projet :

```bash
docker compose config
docker compose up -d --build
docker compose ps
```

- `docker compose config` vérifie la configuration Compose.
- `docker compose up -d --build` construit les images nécessaires et démarre les services.
- `docker compose ps` affiche leur état.

Consulter les logs :

```bash
docker compose logs -f nifi
docker compose logs -f kafka
docker compose logs -f postgres
docker compose logs -f namenode
docker compose logs -f spark-master
```

Arrêter les services :

```bash
docker compose down
```

Cette commande conserve normalement les volumes persistants. Évite `docker compose down -v` si tu souhaites garder les données.

### 5.3. Interfaces

Les ports exacts dépendent de `docker-compose.yml`.

| Service           | Adresse typique                  |
| ----------------- | -------------------------------- |
| NiFi              | `https://localhost:8443/nifi`    |
| Airflow local     | `http://localhost:8080`          |
| FastAPI           | `http://localhost:8000`          |
| Documentation API | `http://localhost:8000/docs`     |
| Spark Master UI   | port hôte configuré dans Compose |

Airflow et Spark peuvent entrer en conflit s'ils utilisent tous les deux le port hôte `8080`. Vérifie les ports publiés dans Compose.

## 6. Génération et stockage des données

Le générateur est situé dans `scripts/generate_data.py`. Les fichiers générés sont organisés sous `data/generated/`, notamment par catégories, clients, produits et transactions.

Avant de générer ou de remplacer des fichiers :

```bash
python3 scripts/generate_data.py --help
```

Si le script ne prend pas en charge `--help`, consulte son code pour connaître les paramètres disponibles. Vérifie également s'il écrase des fichiers existants.

### 6.1. Organisation logique dans HDFS

```text
/data/ecommerce/
├── raw/
│   ├── categories/
│   ├── customers/
│   ├── products/
│   └── transactions/
├── silver/
│   ├── categories/
│   ├── customers/
│   ├── products/
│   └── transactions/
├── gold/
└── quarantine/
    └── transactions/
        ├── invalid/
        └── waiting_reference/
```

Lister les fichiers Raw :

```bash
docker exec training_namenode \
  hdfs dfs -ls -R /data/ecommerce/raw
```

Consulter Silver et Gold :

```bash
docker exec training_namenode \
  hdfs dfs -ls -R /data/ecommerce/silver

docker exec training_namenode \
  hdfs dfs -ls -R /data/ecommerce/gold
```

Vérifier l'espace et la réplication :

```bash
docker exec training_namenode hdfs dfsadmin -report
```

Consulter la taille des transactions Raw :

```bash
docker exec training_namenode \
  hdfs dfs -du -h /data/ecommerce/raw/transactions
```

Les transactions Silver sont partitionnées par `transaction_date` pour faciliter les lectures analytiques. Cela n'impose pas que chaque fichier Raw corresponde à une seule journée. `source_file` sert à conserver la provenance du lot.

## 7. Pipeline batch : Airflow et Spark

### 7.1. Fonctionnement

Le DAG `ecommerce_pipeline`, situé dans `airflow/dags/ecommerce_pipeline.py`, orchestre le traitement des nouveaux fichiers.

Le flux logique est :

1. Détecter un fichier ou un lot non traité.
2. Lancer les traitements Spark nécessaires.
3. Nettoyer et valider les transactions.
4. Vérifier les références clients et produits.
5. Écrire les données valides dans Silver.
6. Envoyer les données invalides ou en attente de référence vers Quarantine.
7. Produire les agrégats Gold.
8. Effectuer les contrôles qualité et enregistrer le statut du traitement.

Les dépendances exactes entre tâches sont définies dans le DAG.

### 7.2. Démarrer Airflow localement

Si l'environnement virtuel `.venv` est déjà configuré, depuis la racine du projet :

```bash
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW_CONFIG="$PWD/airflow/airflow.cfg"
airflow standalone
```

Laisse ce terminal ouvert. Pour utiliser la CLI dans un second terminal, réactive l'environnement virtuel et redéfinis les variables avec le chemin réel du projet.

### 7.3. Commandes Airflow

Lister les DAGs :

```bash
airflow dags list
```

Lister les tâches d'un DAG :

```bash
airflow tasks list ecommerce_pipeline
```

Lister les exécutions :

```bash
airflow dags list-runs ecommerce_pipeline
```

Déclencher une exécution :

```bash
airflow dags trigger ecommerce_pipeline
```

Les logs détaillés des tâches se trouvent sous `airflow/logs/`. L'interface Web est utile pour examiner l'état des tâches, leurs dépendances et leurs erreurs.

### 7.4. Jobs Spark

Les traitements se trouvent dans `spark/jobs/` :

| Fichier                     | Rôle                                                            |
| --------------------------- | --------------------------------------------------------------- |
| `dimensions_to_silver.py`   | Nettoyage et validation des dimensions                          |
| `transactions_to_silver.py` | Nettoyage, validation métier et enrichissement des transactions |
| `silver_to_gold.py`         | Production des agrégats analytiques                             |
| `quality_checks.py`         | Contrôles qualité                                               |
| `check_silver.py`           | Vérifications de Silver                                         |
| `count_silver_rows.py`      | Comptage des lignes Silver                                      |

Exemple de vérification du lancement d'un job depuis le conteneur Spark Master :

```bash
docker exec training_spark_master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/transactions_to_silver.py --help
```

Pour exécuter un traitement réel, consulte les arguments acceptés par le script et indique le chemin HDFS attendu. Évite de relancer un job en écriture append sans vérifier son comportement en cas de reprise.

### 7.5. Idempotence

Un retry Airflow ou un rejeu de fichier ne doit pas créer de doublons. Le suivi des fichiers traités est utile, mais ne garantit pas à lui seul l'unicité des transactions. Les jobs doivent également appliquer une stratégie d'écriture idempotente au niveau des enregistrements.

## 8. API et ingestion temps réel

L'API se trouve dans `api/app/` :

- `main.py` expose les routes ;
- `schemas.py` définit les données attendues et retournées ;
- `services/transaction_service.py` contient la logique métier et la communication avec PostgreSQL et NiFi.

### 8.1. Démarrer l'API

Si le service `api` est déclaré dans `docker-compose.yml` :

```bash
docker compose up -d --build api
docker compose logs -f api
```

Vérifier sa santé :

```bash
curl -i http://localhost:8000/health
```

Ouvrir la documentation interactive :

```text
http://localhost:8000/docs
```

### 8.2. Consulter les références

```bash
curl -s http://localhost:8000/api/products
curl -s http://localhost:8000/api/customers
```

### 8.3. Tester une transaction

Les identifiants doivent correspondre à des produits et clients réellement présents dans PostgreSQL.

```bash
curl -i -X POST http://localhost:8000/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{
    "customer_id": 1001,
    "product_id": 25,
    "quantity": 2,
    "discount_rate": 0.05,
    "country": "TG",
    "payment_method": "mobile_money"
  }'
```

L'API calcule le montant à partir du prix du produit récupéré dans PostgreSQL. Le contrat de l'événement inclut notamment l'identifiant de transaction, les identifiants client et produit, la quantité, le prix unitaire, la remise, le montant, le pays, le statut, l'horodatage, le moyen de paiement et la source.

### 8.4. Vérifier NiFi

L'URL configurée pour l'API utilise actuellement une valeur par défaut de la forme :

```text
http://nifi:8085/api/transactions
```

`nifi` est le nom DNS du service dans le réseau Docker. Le port `8085` est le port d'écoute interne configuré pour le flux HTTP NiFi.

Consulter les logs :

```bash
docker compose logs --tail=100 api
docker compose logs --tail=100 nifi
```

Un code HTTP 2xx confirme que le endpoint a accepté la requête. Il ne prouve pas à lui seul que les données sont présentes dans HDFS et Kafka : les deux destinations doivent être vérifiées indépendamment.

## 9. Kafka

Le topic prévu pour les événements est `ecommerce-transactions`, avec trois partitions dans l'environnement de formation.

Les partitions permettent de répartir les messages et le travail entre consommateurs. La répartition dépend notamment de la clé des messages et ne garantit pas un équilibre parfait.

Les scripts et consommateurs se trouvent dans :

```text
kafka/
├── consumer/
│   ├── consumer.py
│   └── consumer_csv.py
├── create_topics.sh
└── scripts/
```

Consulter le script de création des topics :

```bash
sed -n '1,220p' kafka/create_topics.sh
```

Repérer les outils Kafka disponibles dans le conteneur :

```bash
docker exec training_kafka \
  sh -lc 'find / -name kafka-topics.sh 2>/dev/null | head'
```

Une fois le chemin identifié, utiliser `kafka-topics.sh` avec le broker interne `kafka:9092` pour lister ou inspecter les topics.

Consulter les consommateurs :

```bash
sed -n '1,240p' kafka/consumer/consumer.py
sed -n '1,240p' kafka/consumer/consumer_csv.py
```

Dans le réseau Docker, les clients utilisent le nom du service et le port interne. Depuis l'hôte, ils doivent utiliser le port publié et la configuration d'écoute externe de Kafka. `kafka:9092` et `localhost:<port_publié>` ne sont donc pas interchangeables.

## 10. PostgreSQL

Les scripts d'initialisation du schéma sont situés dans `postgres/init/` :

- `001_schema.sql` : schéma de données ;
- `002_analytics.sql` : objets analytiques.

Le script `db/load_reference_data.py` concerne le chargement des données de référence.

Le conteneur PostgreSQL est actuellement nommé `airflow_training_postgres`.

Ouvrir une session SQL avec les valeurs configurées dans l'environnement :

```bash
docker exec -it airflow_training_postgres \
  psql -U <utilisateur> -d <base>
```

Remplace `<utilisateur>` et `<base>` par les valeurs configurées localement. Ne publie pas de mots de passe dans le README ou dans le dépôt.

Quelques requêtes utiles :

```sql
-- Tables du schéma courant
\dt

-- Nombre de transactions
SELECT COUNT(*) FROM transactions;

-- Dernières transactions
SELECT transaction_id, customer_id, product_id, amount, status, timestamp
FROM transactions
ORDER BY timestamp DESC
LIMIT 10;

-- Vérification des référentiels
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM products;
```

Confirme les noms exacts des tables et des colonnes dans `postgres/init/001_schema.sql`.

## 11. Visualisation avec Metabase

Metabase est l'outil de restitution utilisé pour explorer les tables analytiques et construire les dashboards. Il lit les tables Gold produites par Spark et ne modifie jamais les données.

### 11.1. Rôle et port

Metabase tourne dans un conteneur dédié et publie son interface sur le port `3000`.

| Service  | Adresse                 |
| -------- | ----------------------- |
| Metabase | `http://localhost:3000` |

Metabase utilise lui-même PostgreSQL pour stocker sa configuration interne. Deux bases distinctes coexistent donc dans le même serveur PostgreSQL :

- `metabase_app` : configuration interne de Metabase (utilisateurs, dashboards, questions, historique). Cette base ne doit pas être interrogée manuellement.
- `${POSTGRES_DB}` : base e-commerce contenant les référentiels, les transactions opérationnelles et les tables Gold. C'est celle-ci que Metabase doit interroger pour la BI.

Ne confonds pas les deux : une question Metabase s'exécute toujours sur la base e-commerce, pas sur `metabase_app`.

### 11.2. Variables d'environnement

Metabase exige une clé de chiffrement pour les identifiants qu'il stocke dans sa base. Sans cette clé, il refuse de démarrer.

Dans `.env` :

```bash
MB_ENCRYPTION_SECRET_KEY=<clé générée>
```

### 11.3. Création de la base applicative

Metabase crée ses tables automatiquement, mais pas la base elle-même. Sur un volume déjà existant, ce script n'est pas rejoué : il faut créer la base manuellement une seule fois.

```bash
docker exec -i airflow_training_postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "CREATE DATABASE metabase_app;"
```

#### Vérifier

```bash
docker exec -i airflow_training_postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\l" | grep metabase_app
```

### 11.4. Démarrer Metabase

```bash
docker compose up -d postgres metabase
docker compose logs -f metabase
```

Attendre la ligne :
`*INFO core.core :: Metabase Initialization COMPLETE*`

### 11.5. Export et versionnement des dashboards

Le script scripts/export_metabase.sh effectue cet export.

```bash
export METABASE_USER="admin@exemple.com"
export METABASE_PASSWORD="<mot_de_passe>"
export METABASE_URL="http://localhost:3000"
./scripts/export_metabase.sh
```

Les fichiers sont écrits dans `metabase/dashboards/`

## 12. Commandes de diagnostic

État des services :

```bash
docker compose ps
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

Logs :

```bash
docker compose logs --tail=100 api
docker compose logs --tail=100 nifi
docker compose logs --tail=100 kafka
docker compose logs --tail=100 postgres
docker compose logs --tail=100 spark-master
```

État de HDFS :

```bash
docker exec training_namenode hdfs dfsadmin -report
docker exec training_namenode hdfs dfs -ls -R /data/ecommerce/raw
```

État d'Airflow :

```bash
airflow dags list
airflow dags list-runs ecommerce_pipeline
```

En cas d'erreur, examine en priorité les logs de la tâche Airflow, ceux du service concerné, les chemins HDFS et les noms DNS/ports utilisés entre les conteneurs.

## 13. Points d'attention

### 13.1. Références disponibles dans Silver

Une transaction peut arriver avant que son client ou son produit ait été traité dans Silver. Le pipeline distingue donc les données réellement invalides des transactions valides dont les références sont momentanément absentes.

Les transactions en attente de référence doivent pouvoir être retraitées après l'arrivée des dimensions. Une référence absente de Silver ne signifie pas automatiquement que la transaction est incorrecte.

L'API peut vérifier les produits dans PostgreSQL opérationnel, mais le traitement analytique doit toujours contrôler la cohérence des références utilisées dans Silver.

### 13.2. Livraison des événements

Si l'API enregistre une transaction dans PostgreSQL mais échoue ensuite à l'envoyer à NiFi, les systèmes peuvent diverger. Un simple journal d'erreur ne garantit pas la livraison. Une approche plus robuste serait un **outbox pattern** ou une file de publication persistante, accompagnée d'une stratégie de reprise.

### 13.3. Identifiants uniques

Un compteur Python en mémoire repart de sa valeur initiale au redémarrage et ne garantit pas l'unicité entre plusieurs processus. Pour un fonctionnement robuste, l'identifiant doit être produit par un mécanisme globalement unique, par exemple une séquence persistante ou un UUID compatible avec le modèle de données.

### 13.4. Doublons et reprises

Kafka, les retries Airflow et les rejeux peuvent conduire à retraiter un événement. Il faut définir une clé unique et une stratégie idempotente pour les écritures dans PostgreSQL et Silver. Ne revendique pas une garantie « exactly-once » de bout en bout sans démontrer comment les écritures, les offsets et les reprises sont coordonnés.

### 13.5. Réseau et secrets

- Dans Docker, utiliser le nom du service et le port interne.
- Depuis l'hôte, utiliser le port publié.
- Ne versionner ni mots de passe ni clés API.
- Ne supprimer pas les volumes Docker si les données doivent être conservées.
- Vérifier qu'un générateur n'écrasera pas les fichiers existants avant de le lancer.

## 14. Améliorations prévues

- Finaliser le flux NiFi avec écriture de chaque événement dans HDFS Raw et Kafka.
- Mettre en place une stratégie fiable de publication entre l'enregistrement opérationnel et le flux d'événements.
- Ajouter des tests automatisés pour l'API, les règles métier et les traitements Spark.
- Finaliser les dashboards Metabase : Executive, Ventes & temps, Produits & catégories, Clients & paiement.
- Versionner la configuration des dashboards via `scripts/export_metabase.sh`.

---

## Référence rapide

```bash
# Démarrer les services
docker compose up -d --build

# Vérifier leur état
docker compose ps

# Vérifier l'API
curl -i http://localhost:8000/health

# Documentation API
# http://localhost:8000/docs

# Lancer Airflow localement
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW_CONFIG="$PWD/airflow/airflow.cfg"
airflow standalone

# Déclencher le pipeline
airflow dags trigger ecommerce_pipeline

# Vérifier HDFS
docker exec training_namenode hdfs dfsadmin -report
docker exec training_namenode hdfs dfs -ls -R /data/ecommerce/raw

# Arrêter les services sans supprimer les volumes
docker compose down
```
