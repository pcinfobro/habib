#!/bin/bash

# ============================================
# Simplified Docker Migration Script
# Creates EXACT replica - Windows Compatible
# ============================================

set -e

echo "🚀 Starting Docker Container Migration Process..."
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
        echo "     ✅ Exported: images/${safe_name}.tar"
    else
        echo "  ⚠️  Image not found: $image"
    fi
done

# ============================================
# STEP 2: Create Volume Backup Commands
# ============================================
echo ""
echo "💾 STEP 2: Creating volume backup script..."

declare -a VOLUMES=(
    "ecommerce-data-pipeline_datanode1_data"
    "ecommerce-data-pipeline_datanode2_data"
    "ecommerce-data-pipeline_namenode_data"
    "ecommerce-data-pipeline_postgres_airflow_data"
    "ecommerce-data-pipeline_postgres_hive_data"
    "ecommerce-data-pipeline_postgres_powerbi_data"
    "kdc_data"
    "keytabs_data"
)

# Create volume backup script
cat > backup-volumes.sh << 'EOF'
#!/bin/bash
echo "💾 Backing up Docker volumes..."
mkdir -p volumes

declare -a VOLUMES=(
    "ecommerce-data-pipeline_datanode1_data"
    "ecommerce-data-pipeline_datanode2_data"
    "ecommerce-data-pipeline_namenode_data"
    "ecommerce-data-pipeline_postgres_airflow_data"
    "ecommerce-data-pipeline_postgres_hive_data"
    "ecommerce-data-pipeline_postgres_powerbi_data"
    "kdc_data"
    "keytabs_data"
)

for volume in "${VOLUMES[@]}"; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then
        echo "  💾 Backing up volume: $volume"
        docker run --rm -v "$volume":/source -v "$(pwd)/volumes":/backup busybox tar -czf "/backup/${volume}.tar.gz" -C /source .
        if [ -f "volumes/${volume}.tar.gz" ]; then
            echo "     ✅ Backup created: ${volume}.tar.gz"
        else
            echo "     ❌ Backup failed: ${volume}.tar.gz"
        fi
    else
        echo "  ⚠️  Volume not found: $volume"
    fi
done
EOF

chmod +x backup-volumes.sh
echo "  📝 Created volume backup script: backup-volumes.sh"

# ============================================
# STEP 3: Copy Configuration Files
# ============================================
echo ""
echo "📋 STEP 3: Copying configuration files..."

cd ..
mkdir -p "$MIGRATION_DIR/config"

# Copy essential files
cp docker-compose.yml "$MIGRATION_DIR/config/" 2>/dev/null && echo "  ✅ Copied docker-compose.yml" || echo "  ⚠️  docker-compose.yml not found"
cp hadoop.env "$MIGRATION_DIR/config/" 2>/dev/null && echo "  ✅ Copied hadoop.env" || echo "  ⚠️  hadoop.env not found"
cp Dockerfile.* "$MIGRATION_DIR/config/" 2>/dev/null && echo "  ✅ Copied Dockerfiles" || echo "  ⚠️  Dockerfiles not found"
cp requirements*.txt "$MIGRATION_DIR/config/" 2>/dev/null && echo "  ✅ Copied requirements files" || echo "  ⚠️  requirements files not found"

# Copy directories
[ -d "config" ] && cp -r config "$MIGRATION_DIR/config/" && echo "  ✅ Copied config/ directory"
[ -d "dags" ] && cp -r dags "$MIGRATION_DIR/config/" && echo "  ✅ Copied dags/ directory"
[ -d "powerbi-config" ] && cp -r powerbi-config "$MIGRATION_DIR/config/" && echo "  ✅ Copied powerbi-config/ directory"
[ -d "hadoop-config" ] && cp -r hadoop-config "$MIGRATION_DIR/config/" && echo "  ✅ Copied hadoop-config/ directory"
[ -d "security" ] && cp -r security "$MIGRATION_DIR/config/" && echo "  ✅ Copied security/ directory"

# ============================================
# STEP 4: Generate Container Information
# ============================================
echo ""
echo "📊 STEP 4: Generating system information..."

cd "$MIGRATION_DIR"

# Get current container status
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" > container-status.txt
echo "  ✅ Created container-status.txt"

# Get current images
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.Size}}" > image-list.txt
echo "  ✅ Created image-list.txt"

# Get current volumes
docker volume ls > volume-list.txt
echo "  ✅ Created volume-list.txt"

# ============================================
# STEP 5: Create Restoration Script
# ============================================
echo ""
echo "📝 STEP 5: Creating restoration script..."

cat > restore-on-new-laptop.sh << 'EOF'
#!/bin/bash

# ============================================
# Docker Container Restoration Script
# Restores EXACT replica on Linux laptop
# ============================================

set -e

echo "🔄 Starting Docker Container Restoration on Linux..."
echo ""

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose >/dev/null 2>&1; then
    echo "❌ docker-compose not found. Installing..."
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
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
    echo "  ✅ All custom images loaded"
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
    "ecommerce-data-pipeline_namenode_data"
    "ecommerce-data-pipeline_postgres_airflow_data"
    "ecommerce-data-pipeline_postgres_hive_data"
    "ecommerce-data-pipeline_postgres_powerbi_data"
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
            docker run --rm -v "$volume":/target -v "$(pwd)/volumes":/backup busybox tar -xzf "/backup/${volume}.tar.gz" -C /target
            echo "     ✅ Restored: $volume"
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
    echo "     You can set them now or later:"
    echo "     export POWERBI_CLIENT_ID='your_client_id'"
    echo "     export POWERBI_CLIENT_SECRET='your_client_secret'"
    echo "     export POWERBI_TENANT_ID='your_tenant_id'"
    echo ""
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
    
    sleep 10
    
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
    echo "   - Kafka: localhost:9092"
    echo "   - PostgreSQL: localhost:5432"
    echo ""
    echo "🔍 Useful commands:"
    echo "   - Check logs: docker-compose logs -f [service_name]"
    echo "   - Stop all: docker-compose down"
    echo "   - Restart: docker-compose restart [service_name]"
    
else
    echo "  ❌ docker-compose.yml not found!"
fi

echo ""
echo "🎉 Your exact Docker replica is now running on this Linux laptop!"
EOF

chmod +x restore-on-new-laptop.sh
echo "  ✅ Created restore-on-new-laptop.sh"

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
Target: Linux Laptop

CONTAINERS TO MIGRATE (16 total):
$(cat container-status.txt | tail -n +2)

CUSTOM IMAGES EXPORTED (5 total):
$(ls -la images/ 2>/dev/null | grep -v "^total" | grep -v "^d" | awk '{print $9, $5}' || echo "No custom images found")

VOLUMES TO BACKUP (8 total):
- ecommerce-data-pipeline_datanode1_data
- ecommerce-data-pipeline_datanode2_data  
- ecommerce-data-pipeline_namenode_data
- ecommerce-data-pipeline_postgres_airflow_data
- ecommerce-data-pipeline_postgres_hive_data
- ecommerce-data-pipeline_postgres_powerbi_data
- kdc_data
- keytabs_data

MIGRATION PACKAGE CONTENTS:
- images/ (Custom Docker images)
- volumes/ (Will contain volume backups)
- config/ (All configuration files)
- restore-on-new-laptop.sh (Restoration script)
- backup-volumes.sh (Volume backup script)

NEXT STEPS:
1. Run: ./backup-volumes.sh (to backup volumes)
2. Copy entire migration folder to Linux laptop
3. On Linux laptop, run: ./restore-on-new-laptop.sh
4. Set PowerBI environment variables if needed

VERIFICATION COMMANDS:
- docker ps -a
- docker volume ls
- docker-compose ps

===========================================
EOF

echo "  ✅ Created migration-summary.txt"

echo ""
echo "✅ MIGRATION PREPARATION COMPLETE!"
echo ""
echo "📁 Migration package created in: $MIGRATION_DIR"
echo "📦 Total size: $(du -sh "$MIGRATION_DIR" | cut -f1)"
echo ""
echo "🎯 IMPORTANT: Run the volume backup now:"
echo "   cd $MIGRATION_DIR"
echo "   ./backup-volumes.sh"
echo ""
echo "📋 Then copy the entire folder to your Linux laptop and run:"
echo "   ./restore-on-new-laptop.sh"
echo ""
echo "📊 Summary report: $MIGRATION_DIR/migration-summary.txt"
echo ""
echo "🎉 This will create an EXACT replica of your 16 Docker containers!"
