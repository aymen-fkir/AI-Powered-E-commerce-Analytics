# Go Client 

## 📁 Project Structure

```
├── cmd/
│   └── main.go                 # Application entry point
├── internal/                   # Private application packages
│   ├── models/
│   │   └── types.go           # Data models and structures
│   ├── extract/
│   │   └── extractor.go       # Data extraction from storage
│   ├── enrichment/
│   │   └── enricher.go        # AI-powered data enrichment
│   └── load/
│       └── loader.go          # Data loading and storage
├── pkg/
│   └── utils/
│       └── utils.go           # Shared utility functions
├── go.mod                     # Go module definition
├── go.sum                     # Go module dependencies
```

## 🏗️ Architecture Overview

### **Separation of Concerns**
- **Models**: All data structures and types
- **Extract**: Data extraction from Supabase storage
- **Enrichment**: AI processing using llama.cpp
- **Load**: Data merging and storage operations
- **Utils**: Common utilities and helpers


## 🚀 Usage

### Running the Application
```bash
# From the project root
go run cmd/main.go
```

### Building the Application
```bash
# Build binary
go build -o bin/etl-pipeline cmd/main.go

# Run binary
./bin/etl-pipeline
```

## 📦 Package Details

### **cmd/main.go**
- Application entry point
- Orchestrates the entire ETL pipeline
- Handles client initialization
- Manages pipeline execution flow

### **internal/models**
- `Product`: Raw product data structure
- `Item`: Processed item for AI analysis
- `ItemReview`: AI-generated review structure
- `Response`: AI API response format
- `MergedResponse`: Final merged data structure

### **internal/extract**
- `Extractor`: Manages data extraction from storage
- File listing and downloading
- Data transformation and batching
- Concurrent file processing

### **internal/enrichment**
- `Enricher`: Handles AI-powered data processing
- Prompt generation and management
- OpenAI API integration
- Batch processing with retry logic

### **internal/load**
- `Loader`: Manages data loading and storage
- Data merging operations
- Batch uploading to storage
- File cleanup operations

### **pkg/utils**
- Common utility functions
- Logging with mutex protection
- Generic transformation functions
- JSON schema generation

## 🔧 Configuration

The application uses environment variables for configuration:

```bash
mv .example.env .env
# add the supbase credential 
```

## 🔄 Pipeline Flow

1. **Extract Phase**: Download and process files from Supabase storage
2. **Enrichment Phase**: Process data through AI models for classification and reviews
3. **Load Phase**: Merge data and upload processed results
4. **Cleanup Phase**: (Optional) Remove processed files

