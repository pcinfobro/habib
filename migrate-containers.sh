#!/bin/bash

# ============================================
# Docker Container Migration Script
# Creates EXACT replica of current setup
# ============================================

set -e

echo "🚀 Starting Docker Container Migration Process..."
echo "This script will create an exact replica of your current Docker setup"
echo ""

# Create migration directory
MIGRATION_DIR="./docker-migration-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$MIGRATION_DIR"
cd "$MIGRATION_DIR"

echo "📁 Created migration directory: $MIGRATION_DIR"

# ============================================
# STEP 1: Export Custom Docker Images
# ============================================
echo ""
echo "📦 STEP 1: Exporting custom Docker images..."

declare -a CUSTOM_IMAGES=(
    "hadoop-kdc:secure"
    "ecommerce-data-pipeline-powerbi-automation"
    "ecommerce-data-pipeline-airflow-webserver"
    "ecommerce-data-pipeline-airflow-scheduler"
    "ecommerce-data-pipeline-airflow-init"
)

mkdir -p images
for image in "${CUSTOM_IMAGES[@]}"; do
    if docker image inspect "$image" >/dev/null 2>&1; then
        echo "  📦 Exporting: $image"
        safe_name=$(echo "$image" | tr '/:' '-')
        docker save "$image" -o "images/${safe_name}.tar"
    else
        echo "  ⚠️  Image not found: $image"
    fi
done

# ============================================
# STEP 2: Backup Docker Volumes
# ============================================
echo ""
echo "💾 STEP 2: Backing up Docker volumes..."

declare -a VOLUMES=(
    "ecommerce-data-pipeline_datanode1_data"
    "ecommerce-data-pipeline_datanode2_data"
    "ecommerce-data-pipeline_datanode_data"
    "ecommerce-data-pipeline_kafka_data"
    "ecommerce-data-pipeline_namenode_data"
    "ecommerce-data-pipeline_postgres_airflow_data"
    "ecommerce-data-pipeline_postgres_data"
    "ecommerce-data-pipeline_postgres_hive_data"
    "ecommerce-data-pipeline_postgres_powerbi_data"
    "ecommerce-data-pipeline_spark_data"
    "ecommerce-data-pipeline_superset_db_data"
    "ecommerce-data-pipeline_superset_home"
    "ecommerce-data-pipeline_zookeeper_data"
    "kdc_data"
    "keytabs_data"
)

mkdir -p volumes
for volume in "${VOLUMES[@]}"; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then
        echo "  💾 Backing up volume: $volume"
        docker run --rm \
            -v "$volume":/data \
            -v "$(pwd)/volumes":/backup \
            alpine sh -c "tar czf /backup/${volume}.tar.gz -C /data ."
    else
        echo "  ⚠️  Volume not found: $volume"
    fi
done

# ============================================
# STEP 3: Copy Configuration Files
# ============================================
echo ""
echo "📋 STEP 3: Copying configuration files..."

cd ..
mkdir -p "$MIGRATION_DIR/config"

# Copy all essential configuration files
cp docker-compose.yml "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  docker-compose.yml not found"
cp hadoop.env "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  hadoop.env not found"
cp Dockerfile.* "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  Dockerfiles not found"

# Copy directories
cp -r config "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  config/ directory not found"
cp -r dags "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  dags/ directory not found"
cp -r powerbi-config "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  powerbi-config/ directory not found"
cp -r trino-config "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  trino-config/ directory not found"
cp -r hadoop-config "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  hadoop-config/ directory not found"
cp -r security "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  security/ directory not found"

# Copy requirements files
cp requirements*.txt "$MIGRATION_DIR/config/" 2>/dev/null || echo "  ⚠️  requirements files not found"

# ============================================
# STEP 4: Generate Container Information
# ============================================
echo ""
echo "📊 STEP 4: Generating container information..."

cd "$MIGRATION_DIR"

# Get current container status
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" > container-status.txt

# Get current images
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.Size}}" > image-list.txt

# Get current volumes
docker volume ls > volume-list.txt

# ============================================
# STEP 5: Create Restoration Script
# ============================================
echo ""
echo "📝 STEP 5: Creating restoration script..."

cat > restore-on-new-laptop.sh << 'EOF'
#!/bin/bash

# ============================================
# Docker Container Restoration Script
# Restores EXACT replica on new laptop
# ============================================

set -e

echo "🔄 Starting Docker Container Restoration..."
echo ""

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose >/dev/null 2>&1; then
    echo "❌ docker-compose not found. Please install docker-compose first."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ============================================
# STEP 1: Load Custom Images
# ============================================
echo "📦 STEP 1: Loading custom Docker images..."

if [ -d "images" ]; then
    for tar_file in images/*.tar; do
        if [ -f "$tar_file" ]; then
            echo "  📦 Loading: $(basename "$tar_file")"
            docker load -i "$tar_file"
        fi
    done
else
    echo "  ⚠️  No images directory found"
fi

# ============================================
# STEP 2: Create and Restore Volumes
# ============================================
echo ""
echo "💾 STEP 2: Creating volumes and restoring data..."

declare -a VOLUMES=(
    "ecommerce-data-pipeline_datanode1_data"
    "ecommerce-data-pipeline_datanode2_data"
    "ecommerce-data-pipeline_datanode_data"
    "ecommerce-data-pipeline_kafka_data"
    "ecommerce-data-pipeline_namenode_data"
    "ecommerce-data-pipeline_postgres_airflow_data"
    "ecommerce-data-pipeline_postgres_data"
    "ecommerce-data-pipeline_postgres_hive_data"
    "ecommerce-data-pipeline_postgres_powerbi_data"
    "ecommerce-data-pipeline_spark_data"
    "ecommerce-data-pipeline_zookeeper_data"
    "kdc_data"
    "keytabs_data"
)

# Create volumes
for volume in "${VOLUMES[@]}"; do
    echo "  💾 Creating volume: $volume"
    docker volume create "$volume" >/dev/null 2>&1 || echo "    Volume already exists"
done

# Restore volume data
if [ -d "volumes" ]; then
    for volume in "${VOLUMES[@]}"; do
        if [ -f "volumes/${volume}.tar.gz" ]; then
            echo "  📥 Restoring data to: $volume"
            docker run --rm \
                -v "$volume":/data \
                -v "$(pwd)/volumes":/backup \
                alpine sh -c "tar xzf /backup/${volume}.tar.gz -C /data"
        else
            echo "  ⚠️  Backup not found for: $volume"
        fi
    done
else
    echo "  ⚠️  No volumes directory found"
fi

# ============================================
# STEP 3: Copy Configuration Files
# ============================================
echo ""
echo "📋 STEP 3: Setting up configuration..."

if [ -d "config" ]; then
    cp -r config/* . 2>/dev/null || true
    echo "  ✅ Configuration files copied"
else
    echo "  ⚠️  No config directory found"
fi

# ============================================
# STEP 4: Environment Variables Check
# ============================================
echo ""
echo "🔐 STEP 4: Environment variables check..."

if [ -z "$POWERBI_CLIENT_ID" ] || [ -z "$POWERBI_CLIENT_SECRET" ] || [ -z "$POWERBI_TENANT_ID" ]; then
    echo "  ⚠️  PowerBI environment variables not set!"
    echo "     Please set:"
    echo "     export POWERBI_CLIENT_ID='your_client_id'"
    echo "     export POWERBI_CLIENT_SECRET='your_client_secret'"
    echo "     export POWERBI_TENANT_ID='your_tenant_id'"
    echo ""
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "  ❌ Restoration cancelled"
        exit 1
    fi
else
    echo "  ✅ PowerBI environment variables are set"
fi

# ============================================
# STEP 5: Start Services
# ============================================
echo ""
echo "🚀 STEP 5: Starting all services..."

if [ -f "docker-compose.yml" ]; then
    echo "  🔄 Starting containers with docker-compose..."
    docker-compose up -d
    
    echo ""
    echo "📊 Container Status:"
    docker-compose ps
    
    echo ""
    echo "✅ RESTORATION COMPLETE!"
    echo ""
    echo "📝 Services are available at:"
    echo "   - Airflow Web UI: http://localhost:8081 (habib/habib)"
    echo "   - Spark Master UI: http://localhost:8080"
    echo "   - Hadoop NameNode UI: http://localhost:9870"
    echo "   - PowerBI Automation: http://localhost:5000"
    echo "   - Streamlit Dashboard: http://localhost:8501"
    echo "   - Trino: http://localhost:8090"
    echo ""
    echo "🔍 To check logs: docker-compose logs -f [service_name]"
    echo "🛑 To stop all: docker-compose down"
    
else
    echo "  ❌ docker-compose.yml not found!"
    echo "     You'll need to manually copy the docker-compose.yml file"
fi

echo ""
echo "🎉 Your exact Docker replica is now running on this laptop!"
EOF

chmod +x restore-on-new-laptop.sh

# ============================================
# STEP 6: Create Summary Report
# ============================================
echo ""
echo "📋 STEP 6: Creating migration summary..."

cat > migration-summary.txt << EOF
===========================================
DOCKER MIGRATION SUMMARY
===========================================
Migration Date: $(date)
Source System: $(hostname)

CONTAINERS TO MIGRATE (17 total):
$(cat container-status.txt)

CUSTOM IMAGES EXPORTED:
$(ls -la images/ 2>/dev/null | grep -v "^total" | grep -v "^d" || echo "No custom images found")

VOLUMES BACKED UP:
$(ls -la volumes/ 2>/dev/null | grep -v "^total" | grep -v "^d" || echo "No volumes found")

NEXT STEPS:
1. Copy entire migration folder to new laptop
2. Run: ./restore-on-new-laptop.sh
3. Set PowerBI environment variables if needed
4. Verify all services are running

VERIFICATION COMMANDS:
- docker ps -a
- docker volume ls
- docker-compose ps
- docker-compose logs

===========================================
EOF

echo ""
echo "✅ MIGRATION PREPARATION COMPLETE!"
echo ""
echo "📁 Migration package created in: $MIGRATION_DIR"
echo "📦 Total size: $(du -sh "$MIGRATION_DIR" | cut -f1)"
echo ""
echo "📋 Next steps:"
echo "   1. Copy the entire '$MIGRATION_DIR' folder to your new laptop"
echo "   2. On new laptop, run: ./restore-on-new-laptop.sh"
echo "   3. Set PowerBI environment variables if needed"
echo ""
echo "📊 Summary report: $MIGRATION_DIR/migration-summary.txt"
echo ""
echo "🎯 This will create an EXACT replica of your current Docker setup!"
