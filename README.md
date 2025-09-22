# AI-Powered E-commerce Analytics 🛍️🤖

An end-to-end AI-powered analytics platform for e-commerce data processing, featuring automated sentiment analysis, KPI generation, and multi-language client support.

## 🎯 Project Goal & What It Does

This project is a comprehensive data engineering solution designed to:

- **Extract** e-commerce data from various sources (Supabase storage, APIs)
- **Transform** raw data using AI-powered sentiment analysis and automated KPI calculations
- **Load** processed data into databases and storage systems for analytics
- **Provide** multi-language client interfaces (Python, Go) for data interaction
- **Generate** actionable insights from customer reviews, sales data, and user behavior

### Key Features:
- 🤖 **AI-Powered Sentiment Analysis**: Uses local LLM (llama.cpp) for review classification
- 📊 **Automated KPI Generation**: Shop performance, user metrics, and time-series analytics
- 🐳 **Containerized Architecture**: Docker-based microservices for scalability
- 🔄 **Modular ETL Pipeline**: Clean separation of extract, transform, and load operations
- 🌐 **Multi-Client Support**: Python and Go clients 
- 📈 **Real-time Processing**: Streaming and batch processing capabilities

## 📁 Repository Structure

```
AI-Powered-E-commerce-Analytics/
├── README.md                           # This file - Project overview and setup
├── .env.example                        # Environment variables template
├── config.yaml                         # Global configuration file
├── docker-compose.yml                  # Multi-service orchestration
├── .gitignore                          # Git ignore patterns
├── 
├── etl_pipeline/                       # 🔄 Main ETL Pipeline (Python)
│   ├── src/
│   │   └── etl_pipeline/
│   │       ├── main.py                 # Pipeline orchestrator
│   │       ├── models/                 # Pydantic data models
│   │       ├── extract/                # Data extraction module
│   │       ├── transform/              # AI transformation module
│   │       ├── load/                   # Data loading module
│   │       └── utils/                  # Common utilities
│   ├── Dockerfile                      # ETL container config
│   ├── requirements.txt                # Python dependencies
│   └── README.md                       # ETL-specific documentation
│
├── Clients/                            # 👥 Client Applications
│   ├── go/                             # 🔷 Go Client
│   │   ├── cmd/main.go                 # Go application entry point
│   │   ├── internal/                   # Internal Go packages
│   │   │   ├── models/                 # Data structures
│   │   │   ├── extract/                # Data extraction
│   │   │   ├── enrichment/             # AI enrichment
│   │   │   └── load/                   # Data loading
│   │   ├── pkg/utils/                  # Shared utilities
│   │   ├── Dockerfile                  # Go client container
│   │   └── README.md                   # Go client docs
│   │
│   └── python/                         # 🐍 Python Clients
│       ├── llama_cpp_client.py         # Direct llama.cpp integration
│       └── ollama_client.py            # Ollama client wrapper
│
├── collect/                            # 📡 Data Collection Service
│   ├── collector.py                    # Data collection logic
│   ├── Dockerfile                      # Collector container
│   └── requirements.txt                # Collector dependencies
│
├── llama.cpp/                          # 🧠 AI/LLM Infrastructure (Git Submodule)
│   ├── models/                         # LLM model files (.gguf)
│   └── .devops/                        # Docker configs for llama.cpp
│
├── logs/                               # 📋 Application Logs
│   └── http_requests.log               # HTTP request logs
│
└── modelfiles/                         # 📄 Model Configuration Files
    └── Modelfile                       # LLM model definitions
```

### Component Overview:

#### 🔄 **ETL Pipeline** (`etl_pipeline/`)
- **Purpose**: Core data processing engine
- **Technology**: Python with Polars, Pydantic, OpenAI API
- **Features**: Modular architecture, async processing, AI integration

#### 👥 **Client Applications** (`Clients/`)
- **Go Client**: High-performance concurrent processing
- **Python Clients**: Flexible scripting and prototyping

#### 📡 **Data Collector** (`collect/`)
- **Purpose**: Web scraping and data ingestion
- **Features**: Rate limiting, error handling, data validation

#### 🧠 **AI Infrastructure** (`llama.cpp/`)
- **Purpose**: Local LLM server for sentiment analysis
- **Technology**: llama.cpp with CUDA acceleration
- **Models**: Gemma, OpenHermes, and other GGUF models

## 📋 Prerequisites


The storage bucket follow the medalian schema as follow 

```
├── Bronze/                       # raw data
│   ├── old/                      # the raw data that have been Enriched 
│   ├── new/                      # the raw data that waiting to be Enriched
├── Silver/                       # Enriched data
│   ├── processed/                # The data that have been enriched and used to generate kpis
│   ├── to_process/               # The data that have been enriched and waiting to generate kpis
├── gold/                         # final data
│   
```

Create 3 Database in supabase that follow these schema :

#### Database name (user_kpis)
```
{'id': String, 'average_spent': Float64, 'positive_reviews': UInt32, 'negative_reviews': UInt32, 'likeness_score': Float64, 'normalized_likeness_score': Float64}
```

#### Database name (shop_kpis)
```
{'shop_id': String, 'average_profit': Float64, 'positive_reviews': UInt32, 'negative_reviews': UInt32, 'likeness_score': Float64, 'normalized_likeness_score': Float64})
```

#### Database name (date_kpis)
```
{'date': String, 'average_profit_per_day': Float64}
```

## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone --recursive https://github.com/aymen-fkir/AI-Powered-E-commerce-Analytics.git
cd AI-Powered-E-commerce-Analytics
```

### 2. Environment Setup
```bash
# Copy environment template
cp .example.env .env

# Edit with your credentials
nano .env
```

Required environment variables:
```bash
project_url=https://your-project.supabase.co
project_key=your_supabase_service_key
project_jwt_key=your-supabase-jwt-key
api_key=marcove-api-key
```

### 3. Configuration
Edit `config.yaml` to match your setup:
```yaml
# Supabase storage paths
supabase:
  bucketName: 'your-bucket-name'
  
# AI model configuration  
ETLCONFIG:
  model: 'gemma-3-1b'
  base_url: 'http://localhost:8000/v1/'
```

## 🐳 Running with Docker (Recommended)

### Start All Services
```bash
# Build and start all containers
docker-compose up -d

# View logs
docker-compose logs -f
```

### Run Individual Services
```bash
# LLM server only
docker-compose up llama

# ETL pipeline only  
docker-compose up etl

# Go client only
docker-compose up go-client

# Data collector only
docker-compose up collector
```

### Service Endpoints
- **LLM Server**: http://localhost:8000
- **Go Client**: http://localhost:8080  
- **ETL Pipeline**: http://localhost:5000
- **Data Collector**: http://localhost:5050

## 🛠️ Manual Installation

### ETL Pipeline

[ETL](./etl_pipeline/README.md)

### Go Client
[CLIENT](./Clients/go/README.md)

### Data Collector
[COLLECTOR](./collect/README.md)

### llama-cpp
[LLAMA-CPP](./llama.cpp/README.md)


## 🔧 Development

### Adding New Transformations
1. Create new module in `etl_pipeline/src/etl_pipeline/transform/`
2. Implement transformation logic
3. Register in main pipeline orchestrator

### Extending Client Support
1. Create new client directory in `Clients/`
2. Implement core interfaces (extract, transform, load)
3. Add Docker configuration

### Custom AI Models
1. Add model files to `llama.cpp/models/`
2. Update `config.yaml` with model configuration
3. Restart llama service

**Note**: This project demonstrates modern data engineering practices with AI integration. Perfect for learning microservices architecture, containerization, and ML operations.
you can read about my Project in This [blog](https://medium.com/@aymenfkir23/building-an-llm-powered-etl-pipeline-for-review-generation-at-scale-6af943c03feb) 