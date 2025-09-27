# ETL Pipeline - Refactored Architecture 🚀

A modular, production-ready ETL pipeline for e-commerce analytics with AI-powered sentiment analysis and automated KPI generation.

## 📁 Project Structure

```
etl_pipeline/
├── src/
│   └── etl_pipeline/
│       ├── __init__.py              # Package initialization
│       ├── main.py                  # Main application orchestrator
│       ├── models/
│       │   └── __init__.py     
│       │   └── models_schema.py       # Pydantic models and data structures
│       ├── extract/
│       │   └── __init__.py
│       │   └── data_extractor          # Data extraction from Supabase
│       ├── transform/
│       │   └── __init__.py  
│       │   └── data_transformer.py          # AI sentiment analysis & KPI generation
│       ├── load/
│       │   └── __init__.py   
│       │   └── data_loader.py         # Data loading to various destinations
│       └── utils/
│           └── __init__.py  
│       │   └── tools.py          # Common utilities and helpers
├── requirements.txt                 # Python dependencies                      
├── Dockerfile                      # Container configuration              
└── README.md                       # This file
```

## 🏗️ Architecture Overview

### **Modular Design Principles**
- **Single Responsibility**: Each module handles one specific aspect
- **Separation of Concerns**: Clear boundaries between Extract, Transform, Load
- **Dependency Injection**: Components receive dependencies via constructors
- **Configuration Management**: Centralized config with environment overrides

### **Key Components**

#### **📥 Extract Module (`extract/`)**
- **DataExtractor**: Handles file listing and downloading from Supabase storage
- **Features**: Batch processing, error handling, progress tracking
- **Output**: Clean Polars DataFrame ready for transformation

#### **⚙️ Transform Module (`transform/`)**
- **DataTransformer**: AI-powered sentiment analysis and KPI generation
- **Features**: Async processing, structured outputs, batch optimization
- **AI Integration**: OpenAI-compatible API with JSON schema validation

#### **📤 Load Module (`load/`)**
- **DataLoader**: Multi-destination data loading (files, database, storage)
- **Features**: Flexible output formats, database table creation, error recovery
- **Destinations**: Parquet files, Supabase tables, cloud storage

#### **📊 Models Module (`models/`)**
- **Pydantic Models**: Type-safe data structures with validation
- **Configuration**: ETLConfig for centralized settings
- **Schemas**: Request/response models for AI API integration

#### **🛠️ Utils Module (`utils/`)**
- **Common Functions**: Logging, batching, normalization, validation
- **Reusable Logic**: Shared across all modules
- **Error Handling**: Consistent error patterns and logging

## 🚀 Getting Started

### **Prerequisites**
- Python 3.9+
- Docker & Docker Compose
- Supabase account with storage bucket
- llama-cpp server with language model

### **Installation**

#### **Option 1: Direct Installation**
```bash
# Clone the repository
git clone https://github.com/aymen-fkir/AI-Powered-E-commerce-Analytics.git
cd etl_pipeline

# Install dependencies
pip install -r requirements.txt

# Install as editable package
pip install -e .
```

#### **Option 2: Docker Installation**
```bash
# Build and run with Docker Compose
docker-compose up etl

# Or build Docker image only
docker build -t etl-pipeline .
```

### **Configuration**

#### **Environment Variables**
Create a `.env` file:
```bash
project_url=https://your-project.supabase.co
project_key=your_supabase_service_key
```


## 🎯 Usage

### **Command Line Interface**
```bash
# Run with default settings
python -m etl_pipeline.main

# Specify output format
python -m etl_pipeline.main --save-format database

# Set logging level
python -m etl_pipeline.main --log-level DEBUG

# Show help
python -m etl_pipeline.main --help
```

### **Docker Usage**
```bash
# Run complete pipeline
docker-compose up

# Run specific service
docker-compose up etl
```

## 📈 Features

### **🔄 Pipeline Stages**

#### **1. Extract Phase**
- **File Discovery**: Lists files in Supabase storage with sorting
- **Parallel Downloads**: Concurrent file processing for performance
- **Data Validation**: Ensures data quality and completeness
- **Progress Tracking**: Real-time progress with tqdm

#### **2. Transform Phase**
- **Sentiment Analysis**: AI-powered review classification
- **KPI Generation**: Automated metrics for shops, users, and dates
- **Data Enrichment**: Combines raw data with AI insights
- **Schema Validation**: Ensures data consistency with Pydantic

#### **3. Load Phase**
- **Multi-Format Output**: Parquet, JSON, Database tables
- **Flexible Destinations**: Local files, cloud storage, databases
- **Table Management**: Automatic schema creation and data insertion
- **Error Recovery**: Handles partial failures gracefully

### **🤖 AI Integration**
- **OpenAI-Compatible API**: Works with llama.cpp
- **Structured Outputs**: JSON schema validation for reliability
- **Batch Processing**: Optimized for throughput and cost
- **Error Handling**: Robust retry logic and fallback strategies

### **📊 KPI Metrics**
- **Shop KPIs**: Average profit, review sentiment scores
- **User KPIs**: Spending patterns, satisfaction metrics
- **Date KPIs**: Time-series analysis of performance


## 🐳 Docker Services

### **Service Overview**
- **etl**: Main application container
- **llama.cpp**: Local LLM server for AI processing