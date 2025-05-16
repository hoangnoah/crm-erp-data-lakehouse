## Data Lakehouse

#### Technologies:
* MinIO
* Apache Iceberg
* Hive Metastore (Backed by PostgreSQL)
* Apache Spark
* Apache Airflow

### Set up steps:
1. Create bucket

```{bash}
docker compose up minio --build
```

- Go to: http://localhost:9001/
- Login:
    - User: minioadmin
    - Password: minioadmin
- Create a bucket named `warehouse`
- Create an asses key, update it in `spark\.env`

2. Run `postgres-metastore` service

3. Run `hive-metastore` service

4. Run `spark-master` service

5. Run `spark-worker` service

7. Run `dremio` service

6. Test
```{bash}
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  ./spark-scripts/test.py
```
