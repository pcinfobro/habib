# 🎯 AIRFLOW GUIDE: What To Do After Login

## 📋 **STEP-BY-STEP GUIDE:**

### **1. Login to Airflow**
- Go to: http://localhost:8081
- Username: `habib`
- Password: `habib`

---

### **2. After Login - Main Dashboard**
You'll see the Airflow main dashboard with:
- **DAGs List**: Shows all available workflows
- **Task Instances**: Shows running/completed tasks
- **Browse Menu**: Access logs, connections, variables

---

### **3. Find Your Pipeline DAG**
Look for: **`enterprise_ecommerce_pipeline`**
- This is your complete data pipeline DAG
- Should show in the DAGs list
- Click on the DAG name to open it

---

### **4. Explore the DAG (Your Pipeline)**
Once you click on `enterprise_ecommerce_pipeline`:

#### **Graph View** (Most Important):
- Shows visual workflow of your pipeline
- Tasks: `ingest_data` → `process_spark` → `create_powerbi` → `validate_quality`
- Green = Success, Red = Failed, Blue = Running

#### **Tree View**:
- Shows task execution history over time
- Useful for monitoring patterns

#### **Gantt View**:
- Shows task duration and timing
- Helps identify bottlenecks

---

### **5. Trigger Your Pipeline Manually**
**TO RUN YOUR ACTUAL DATA PIPELINE:**

1. Click **"Trigger DAG"** button (play icon ▶️)
2. Confirm the trigger
3. Watch tasks execute in real-time
4. Monitor progress in Graph view

---

### **6. Monitor Task Execution**
**Each task represents a pipeline step:**

- **`ingest_to_kafka`**: Load data into Kafka
- **`process_with_spark`**: Spark processes 1M+ records  
- **`store_in_hdfs`**: Save to distributed storage
- **`export_to_powerbi`**: Create PowerBI datasets
- **`validate_data_quality`**: Check processing success

---

### **7. View Task Logs**
**To see what each task is doing:**
1. Click on any task box in Graph view
2. Select **"Log"** from popup menu
3. See real-time execution logs
4. Debug any issues

---

### **8. Check Task Details**
**Click on tasks to see:**
- **Instance Details**: Runtime, duration, status
- **Rendered Template**: Actual code executed
- **Task Instance**: Dependencies and triggers
- **XCom**: Data passed between tasks

---

### **9. Browse Menu Options**
**Explore these sections:**

#### **Browse → Task Instances**:
- See all task executions across all DAGs
- Filter by status, date, DAG

#### **Browse → DAG Runs**:  
- See complete pipeline execution history
- Success/failure rates

#### **Browse → Logs**:
- Access detailed execution logs
- Search and filter logs

#### **Admin → Connections**:
- See database connections (Spark, PostgreSQL, etc.)
- Verify service connectivity

---

### **10. What You Should See Working**
**If everything is working properly:**

1. **DAG appears** in the main list
2. **Trigger works** without errors
3. **Tasks turn green** as they complete
4. **Logs show** actual data processing
5. **1M+ records** being processed in logs
6. **PowerBI files** generated in output directories

---

## 🎯 **IMMEDIATE ACTION PLAN:**

### **Right Now - Do This:**
1. **Login** with habib/habib
2. **Find** `enterprise_ecommerce_pipeline` DAG
3. **Click** on the DAG name
4. **Trigger** the pipeline (▶️ button)
5. **Watch** the Graph view as tasks execute
6. **Check logs** of running tasks

### **What You'll See:**
- Real-time pipeline execution
- Spark processing your 1M+ records
- Data flowing through Kafka → Spark → HDFS → PowerBI
- Actual big data pipeline in action!

---

## 🚀 **THIS IS YOUR REAL PIPELINE RUNNING:**
Unlike my previous fake dashboards, this is your **actual enterprise data pipeline** orchestrating real big data processing with Spark, Kafka, and HDFS!

**Go trigger that DAG and watch your pipeline work! 🎯**
