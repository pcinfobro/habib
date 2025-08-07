# Docker2. **kafka** (confluentinc/cp-kafka:7.4.0)
3. **powerbi-automation** (custom image: ecommerce-data-pipeline-powerbi-automation)
4. **datanode1** (bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8)
5. **datanode2** (bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8)
6. **namenode** (bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8)
7. **airflow-webserver** (custom image: ecommerce-data-pipeline-airflow-webserver)
8. **airflow-scheduler** (custom image: ecommerce-data-pipeline-airflow-scheduler)
9. **airflow-init** (custom image: ecommerce-data-pipeline-airflow-init)
10. **spark-worker1** (bitnami/spark:3.5.0)
11. **spark-worker2** (bitnami/spark:3.4)
12. **spark-master** (bitnami/spark:3.5.0)
13. **zookeeper** (confluentinc/cp-zookeeper:7.4.0)
14. **postgres-powerbi** (postgres:13)
15. **postgres-airflow** (postgres:13)
16. **postgres-hive** (postgres:13)ation Guide - Exact Replica

## Current Container Status Analysis

Based on the analysis, you have **16 containers** that need to be replicated on your new laptop:

### Stopped/Exited Containers (All need to be replicated):
1. **hadoop-kdc** (custom image: hadoop-kdc:secure)
2. **kafka** (confluentinc/cp-kafka:7.4.0)
4. **powerbi-automation** (custom image: ecommerce-data-pipeline-powerbi-automation)
5. **datanode1** (bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8)
6. **datanode2** (bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8)
7. **namenode** (bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8)
8. **airflow-webserver** (custom image: ecommerce-data-pipeline-airflow-webserver)
9. **airflow-scheduler** (custom image: ecommerce-data-pipeline-airflow-scheduler)
10. **airflow-init** (custom image: ecommerce-data-pipeline-airflow-init)
11. **spark-worker1** (bitnami/spark:3.5.0)
12. **spark-worker2** (bitnami/spark:3.4)
13. **spark-master** (bitnami/spark:3.5.0)
14. **zookeeper** (confluentinc/cp-zookeeper:7.4.0)
15. **postgres-powerbi** (postgres:13)
16. **postgres-airflow** (postgres:13)
17. **postgres-hive** (postgres:13)

## Critical Data to Preserve

### Docker Volumes (Persistent Data):
- `ecommerce-data-pipeline_datanode1_data`
- `ecommerce-data-pipeline_datanode2_data`
- `ecommerce-data-pipeline_namenode_data`
- `ecommerce-data-pipeline_postgres_airflow_data`
- `ecommerce-data-pipeline_postgres_hive_data`
- `ecommerce-data-pipeline_postgres_powerbi_data`
- `kdc_data`
- `keytabs_data`

## Migration Steps for New Laptop

### Step 1: Export All Custom Docker Images
```bash
# Export custom images to tar files
docker save hadoop-kdc:secure -o hadoop-kdc-secure.tar
docker save ecommerce-data-pipeline-powerbi-automation -o powerbi-automation.tar
docker save ecommerce-data-pipeline-airflow-webserver -o airflow-webserver.tar
docker save ecommerce-data-pipeline-airflow-scheduler -o airflow-scheduler.tar
docker save ecommerce-data-pipeline-airflow-init -o airflow-init.tar
```

### Step 2: Backup All Volumes Data
```bash
# Create backup directory
mkdir -p ./volume-backups

# Backup each volume
docker run --rm -v ecommerce-data-pipeline_datanode1_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/datanode1_data.tar.gz -C /data .
docker run --rm -v ecommerce-data-pipeline_datanode2_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/datanode2_data.tar.gz -C /data .
docker run --rm -v ecommerce-data-pipeline_namenode_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/namenode_data.tar.gz -C /data .
docker run --rm -v ecommerce-data-pipeline_postgres_airflow_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/postgres_airflow_data.tar.gz -C /data .
docker run --rm -v ecommerce-data-pipeline_postgres_hive_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/postgres_hive_data.tar.gz -C /data .
docker run --rm -v ecommerce-data-pipeline_postgres_powerbi_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/postgres_powerbi_data.tar.gz -C /data .
docker run --rm -v kdc_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/kdc_data.tar.gz -C /data .
docker run --rm -v keytabs_data:/data -v $(pwd)/volume-backups:/backup alpine tar czf /backup/keytabs_data.tar.gz -C /data .
```

### Step 3: Copy Project Files
Copy the entire project directory including:
- `docker-compose.yml`
- All `Dockerfile.*` files
- `hadoop.env`
- `config/` directory
- `dags/` directory
- `powerbi-config/` directory
- `trino-config/` directory
- All other configuration files

### Step 4: On New Laptop - Restore Everything

#### 4.1: Load Custom Images
```bash
# Load all custom images
docker load -i hadoop-kdc-secure.tar
docker load -i powerbi-automation.tar
docker load -i airflow-webserver.tar
docker load -i airflow-scheduler.tar
docker load -i airflow-init.tar
```

#### 4.2: Create Volumes and Restore Data
```bash
# Create volumes
docker volume create ecommerce-data-pipeline_datanode1_data
docker volume create ecommerce-data-pipeline_datanode2_data
docker volume create ecommerce-data-pipeline_namenode_data
docker volume create ecommerce-data-pipeline_postgres_airflow_data
docker volume create ecommerce-data-pipeline_postgres_hive_data
docker volume create ecommerce-data-pipeline_postgres_powerbi_data
docker volume create kdc_data
docker volume create keytabs_data

# Restore data to volumes
docker run --rm -v ecommerce-data-pipeline_datanode1_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/datanode1_data.tar.gz -C /data
docker run --rm -v ecommerce-data-pipeline_datanode2_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/datanode2_data.tar.gz -C /data
docker run --rm -v ecommerce-data-pipeline_namenode_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/namenode_data.tar.gz -C /data
docker run --rm -v ecommerce-data-pipeline_postgres_airflow_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/postgres_airflow_data.tar.gz -C /data
docker run --rm -v ecommerce-data-pipeline_postgres_hive_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/postgres_hive_data.tar.gz -C /data
docker run --rm -v ecommerce-data-pipeline_postgres_powerbi_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/postgres_powerbi_data.tar.gz -C /data
docker run --rm -v kdc_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/kdc_data.tar.gz -C /data
docker run --rm -v keytabs_data:/data -v $(pwd)/volume-backups:/backup alpine tar xzf /backup/keytabs_data.tar.gz -C /data
```

#### 4.3: Set Environment Variables
Make sure to set PowerBI credentials on new laptop:
```bash
export POWERBI_CLIENT_ID="your_client_id"
export POWERBI_CLIENT_SECRET="your_client_secret"
export POWERBI_TENANT_ID="your_tenant_id"
```

#### 4.4: Start All Containers
```bash
docker-compose up -d
```

## What Gets Transferred:

### ✅ Exact Replicas Include:
- All 17 containers with identical configurations
- All persistent data (databases, logs, configurations)
- All custom Docker images
- All environment configurations
- All network configurations
- All volume mounts and data

### ⚠️ Manual Steps Required:
1. Set PowerBI environment variables on new laptop
2. Ensure Docker and Docker Compose are installed
3. May need to rebuild custom images if architecture differs (x86 vs ARM)

## Verification Commands:
After migration, verify everything is identical:
```bash
# Check all containers are created
docker ps -a

# Check all volumes exist
docker volume ls

# Check all images are loaded
docker images

# Start and verify services
docker-compose up -d
docker-compose ps
```

This will give you an **EXACT replica** of your current setup on the new laptop.
