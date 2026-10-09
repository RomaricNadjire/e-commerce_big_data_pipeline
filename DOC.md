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
