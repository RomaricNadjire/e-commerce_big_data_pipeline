## Créer une connexion pour AirFlow

```bash
airflow connections add postgres_ecommerce \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-login airflow_training \
  --conn-password airflow_training_password \
  --conn-schema ecommerce
```

## Verifier si une connexion existe

```bash
airflow connections get postgres_ecommerce
```

## Supprimer une connexion

```bash
airflow connections delete postgres_ecommerce
```

## HDFS : création des dossiers

```bash
docker exec training_namenode \
  hdfs dfs -mkdir -p \
  /data/ecommerce/raw/categories \
  /data/ecommerce/raw/customers \
  /data/ecommerce/raw/products \
  /data/ecommerce/raw/transactions \
  /data/ecommerce/silver \
  /data/ecommerce/gold
```

## Creation du topic

```bash
docker exec training_kafka   /opt/kafka/bin/kafka-topics.sh   --create   --topic ecommerce-transactions   --bootstrap-server localhost:9092   --partitions 3   --replication-factor 1
```

## Inspecter un topic

```bash
 docker exec training_kafka   /opt/kafka/bin/kafka-topics.sh   --describe   --topic ecommerce-transactions   --bootstrap-server localhost:9092
```

## Executer un consumer d'un consumer group

```bash
docker exec training_kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic ecommerce-transactions \
  --group ecommerce-postgres-group \
  --from-beginning
```

## Vérifier les blocs HDFS

```bash
docker exec training_namenode \
  hdfs fsck /data/ecommerce \
  -files \
  -blocks \
  -locations
```

## Modifier les permissions du dossier RAW

```bash
docker exec training_namenode \
  hdfs dfs -chown -R nifi:supergroup /data/ecommerce/raw
```

## Demarage de air flow

```bash
cd chemin_vers_votre_dossier
source .venv/bin/activate

export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW_CONFIG="$PWD/airflow/airflow.cfg"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"

airflow standalone
```

```bash
docker exec training_api sh -c "apt-get update && apt-get install -y curl"
```

## Créer un répertoire Ivy accessible

```bash
docker exec -u 0 training_spark_master sh -c \
  'mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars && chmod -R 777 /tmp/spark-ivy'
```

## Créer la base metabase_app dans PostgreSQL

```bash
docker exec -i airflow_training_postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "CREATE DATABASE metabase_app;"
```

### Si tu veux vraiment spécifier le propriétaire

```bash
docker exec -i airflow_training_postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "CREATE DATABASE metabase_app OWNER = $POSTGRES_USER;"
```

### Si $POSTGRES_USER n'est pas défini dans ton shell

```bash
docker exec -it airflow_training_postgres bash
# maintenant tu es dans le conteneur
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE metabase_app;"
exit
```

### Puis redémarre Metabase

```bash
docker compose restart metabase
docker compose logs -f metabase
```

### Vérification

```bash
docker exec -i airflow_training_postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\l" | grep metabase_app
```

## Génère la clé une fois avec

```bash
openssl rand -hex 32
```

### Pré-télécharger le jar à le monter

```bash
mkdir -p spark/jars
curl -L -o spark/jars/postgresql-42.7.8.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar
```
