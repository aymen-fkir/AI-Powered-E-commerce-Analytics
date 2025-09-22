# Data Collector Service 📡

A robust data collection service that fetches e-commerce data from external APIs (Mockaroo) and stores it in Supabase storage for further processing by the ETL pipeline.

## 🎯 Purpose & Features

The Data Collector is responsible for:
- **API Data Ingestion**: Fetches mock e-commerce data from Mockaroo API
- **Data Enrichment**: Adds synthetic user IDs and shop IDs to simulate real e-commerce relationships
- **Batch Processing**: Collects data in configurable batches before upload
- **Cloud Storage**: Uploads processed data to Supabase storage buckets
- **Error Handling**: Robust error handling with logging and retry mechanisms
- **Containerized Deployment**: Docker-ready for production environments

### Key Capabilities:
- 🔄 **Continuous Data Collection**: Runs in loop mode for ongoing data ingestion
- 📊 **Data Transformation**: Adds realistic e-commerce metadata (users, shops)
- 🏗️ **Batch Optimization**: Configurable batch sizes for optimal performance
- 📁 **Structured Storage**: Organized file storage with timestamps and unique identifiers
- 🐳 **Production Ready**: Containerized with health checks and monitoring

## 📁 Module Structure

```
collect/
├── collector.py                # Main collector class and logic
├── requirements.txt             # Python dependencies
├── Dockerfile                  # Container configuration
└── README.md                   # This documentation
```

## 🔧 Configuration

The collector uses environment variables and YAML configuration:

### Environment Variables (`.env`)
```bash
project_url=https://your-project.supabase.co
project_key=your_supabase_service_key
api_key=your_mockaroo_api_key
```

### YAML Configuration (`config.yaml`)
```yaml
mockaroo:
  url: 'https://my.api.mockaroo.com/users.json'

supabase:
  bucketName: 'datalake'
  path_raw_data:
    new: 'bronze/new'
    old: 'bronze/old'
```

## 🚀 Usage

### Docker Deployment (Recommended)

#### Using Docker Compose
```bash
# From project root
docker-compose up collector

# View logs
docker-compose logs -f collector
```

#### Direct Docker
```bash
# Build image
docker build -t collector:latest .

# Run container
docker run -d \
  --name data-collector \
  -p 5050:5050 \
  -v /path/to/.env:/app/.env:ro \
  -v /path/to/config.yaml:/app/config.yaml:ro \
  collector:latest
```

### Manual Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export project_url="https://your-project.supabase.co"
export project_key="your_supabase_service_key"
export api_key="your_mockaroo_api_key"

# Run collector
python collector.py
```

## 🏗️ Architecture

### Data Flow

1. **API Fetch** (`getData()`): Retrieves raw data from Mockaroo API
2. **User Enrichment** (`addUsers()`): Adds synthetic user IDs with consistent mapping
3. **Shop Enrichment** (`addShops()`): Adds synthetic shop IDs for e-commerce context
4. **Batch Aggregation**: Collects multiple API responses into batches
5. **Upload** (`upload()`): Stores processed data in Supabase storage
6. **Loop Control**: Manages continuous or single-run operations

### Data Transformation

#### User ID Generation
- **Pool Size**: 5,000 unique user IDs
- **Strategy**: UUID4-based identifiers with deterministic shuffling
- **Consistency**: Same user IDs across different data batches

#### Shop ID Generation
- **Pool Size**: 10,000 unique shop IDs
- **Format**: `shop_{id}` pattern for easy identification
- **Distribution**: Even distribution across all collected data

## 📊 Data Processing

### Input Data Structure
```json
{
  "field1": "value1",
  "field2": "value2",
  // ... original Mockaroo fields
}
```

### Output Data Structure
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "shop_id": "shop_1234",
  "field1": "value1",
  "field2": "value2",
  // ... original fields + synthetic IDs
}
```

### File Naming Convention
```
{storage_path}/{timestamp}_{uuid}.json
```
Example: `bronze/new/2025-09-22T10:30:45_550e8400-e29b-41d4-a716-446655440000.json`

## ⚙️ Configuration Options

### Batch Processing
```python
# Configure batch size (default: 10)
collector.main(loop=True, max_size=20)

# Single run mode
collector.main(loop=False)
```

### Storage Paths
- **New Data**: `bronze/new/` - Fresh data from API
- **Old Data**: `bronze/old/` - Archived processed data

## 🔍 Monitoring & Logging

### Log Levels
- **INFO**: Successful operations, upload confirmations
- **WARNING**: API failures, empty responses
- **ERROR**: Critical failures, configuration issues


## 🤝 Integration

### ETL Pipeline Integration
The collector feeds the ETL pipeline by storing data in the configured Supabase path:
```yaml
# ETL will process files from this location
supabase:
  path_raw_data:
    new: 'bronze/new'  # Collector uploads here
```

### API Dependencies
- **Mockaroo**: Mock data generation service
- **Supabase**: Cloud storage and database platform

**Note**: This collector is designed as a microservice in the larger AI-Powered E-commerce Analytics platform, providing clean, enriched data for downstream processing.