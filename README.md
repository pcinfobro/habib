# 🚀 E-commerce Analytics Hub

A beautiful, modern Big Data pipeline dashboard built with Streamlit, featuring real-time analytics and AI-powered insights for e-commerce data.

![Dashboard Preview](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)

## ✨ Features

- 🎨 **Beautiful Modern UI** - Glassmorphism design with gradient themes
- 📊 **Real-time Analytics** - Interactive data visualization and insights
- 🔄 **Robust Data Processing** - Handles multiple CSV formats with smart encoding detection
- 📈 **Advanced Visualizations** - Plotly-powered charts and graphs
- 🚀 **Big Data Ready** - Designed for integration with Kafka, Spark, and HDFS
- ⚡ **Power BI Integration** - Automated reporting and dashboard generation
- 🔍 **Service Monitoring** - Real-time status of Big Data services

## 🎯 Supported Datasets

The dashboard is optimized for e-commerce datasets, particularly:
- `superstore_dataset.csv` - Main e-commerce data
- `cleaned_superstore_dataset.csv` - Pre-processed data
- `navigator_ft-data_preview.csv` - Navigation analytics
- `MOCK_DATA.csv` - Sample test data

## 🚀 Quick Start

### Option 1: Streamlit Cloud (Recommended)
Visit the deployed app and start analyzing your data instantly!

### Option 2: Local Development
```bash
# Clone the repository
git clone https://github.com/mr-uzairnaseer/habib_big_data.git
cd habib_big_data

# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run streamlit_dashboard.py
```

## 📋 Requirements

- Python 3.8+
- Streamlit 1.28.0+
- Pandas 1.5.0+
- Plotly 5.15.0+

## 🎨 Dashboard Sections

### 📤 Data Upload & Processing
- Drag-and-drop CSV upload
- Automatic format detection
- Real-time data preview
- Smart column mapping

### 📊 Real-time Insights
- Key Performance Indicators
- Sales analytics
- Profit analysis
- Interactive visualizations

### ⚡ Power BI Integration
- Automated report generation
- PBIX file creation
- Data export capabilities

### 🔍 Service Monitoring
- Big Data service status
- Pipeline metrics
- System health checks

### 🗂️ Data Explorer
- Raw data inspection
- Column analysis
- Data quality metrics

## 🛠️ Technologies Used

- **Frontend**: Streamlit with custom CSS
- **Data Processing**: Pandas, NumPy
- **Visualizations**: Plotly
- **Big Data**: Kafka, Spark, HDFS integration
- **Design**: Glassmorphism UI with gradient themes

## 📊 Data Pipeline Architecture

```
CSV Upload → Data Validation → Processing → Analytics → Visualization
     ↓              ↓              ↓           ↓            ↓
  Format      Column Mapping   Cleaning   Insights    Interactive
 Detection      & Validation   & Enrichment  Engine      Dashboard
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Uzair Naseer** - [mr-uzairnaseer](https://github.com/mr-uzairnaseer)

## 🌟 Show your support

Give a ⭐️ if this project helped you!

---

Built with ❤️ using Streamlit and Python

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
