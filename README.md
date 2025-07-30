# E-commerce Data Pipeline - Local Development Setup

## Quick Start

1. **Clone/Create the project:**
   \`\`\`bash
   mkdir ecommerce-data-pipeline
   cd ecommerce-data-pipeline
   \`\`\`

2. **Set up Python environment:**
   \`\`\`bash
   python -m venv venv
   
   # Windows:
   venv\Scripts\activate
   
   # macOS/Linux:
   source venv/bin/activate
   \`\`\`

3. **Install dependencies:**
   \`\`\`bash
   pip install -r requirements.txt
   \`\`\`

4. **Run the pipeline:**
   \`\`\`bash
   python local_pipeline.py
   \`\`\`

## VS Code Setup

1. **Install Extensions:**
   - Python (Microsoft)
   - Python Debugger (Microsoft)
   - Jupyter (Microsoft)

2. **Configure Python Interpreter:**
   - Press `Ctrl+Shift+P` (Windows/Linux) or `Cmd+Shift+P` (macOS)
   - Type "Python: Select Interpreter"
   - Choose the interpreter from your `venv` folder

3. **Debug Configuration:**
   - Use the provided `launch.json` configurations
   - Press `F5` to run with debugging

## Project Structure

\`\`\`
ecommerce-data-pipeline/
├── scripts/                 # Original pipeline modules
│   ├── data_ingestion.py
│   ├── data_processing.py
│   └── ...
├── data/                    # Local data directory
│   ├── input/              # Input data files
│   ├── output/             # Processed output
│   └── logs/               # Log files
├── local_pipeline.py        # Simplified local runner
├── local_config.py         # Local configuration
├── local_storage.py        # Local file storage
├── test_pipeline.py        # Unit tests
├── requirements.txt        # Python dependencies
└── README.md               # This file
\`\`\`

## Running Components

### Full Pipeline
\`\`\`bash
python local_pipeline.py
\`\`\`

### Individual Components
\`\`\`bash
# Test data ingestion
python scripts/data_ingestion.py

# Test data processing
python scripts/data_processing.py

# Run tests
python test_pipeline.py
\`\`\`

### Using VS Code Debugger
1. Open the file you want to debug
2. Set breakpoints by clicking in the gutter
3. Press `F5` or use Run > Start Debugging
4. Choose the appropriate configuration

## Sample Data

The pipeline automatically creates sample data files:
- `data/input/orders.csv` - Sample order data
- `data/input/customers.csv` - Sample customer data  
- `data/input/products.json` - Sample product data

## Output

Processed data is saved to:
- `data/output/` - All processed files
- `data/logs/pipeline.log` - Execution logs

## Troubleshooting

### Common Issues:

1. **Module not found errors:**
   - Ensure virtual environment is activated
   - Check Python interpreter in VS Code

2. **Permission errors:**
   - Make sure you have write permissions to the data directory

3. **Import errors:**
   - Verify all dependencies are installed: `pip list`

### Getting Help:
- Check the logs in `data/logs/pipeline.log`
- Run tests to verify setup: `python test_pipeline.py`
