package main

import (
	"log"
	"os"
	"time"

	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/internal/enrichment"
	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/internal/extract"
	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/internal/load"
	"github.com/joho/godotenv"
	openai "github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/option"
	storage_go "github.com/supabase-community/storage-go"
)

// Pipeline orchestrates the entire ETL process
type Pipeline struct {
	extractor *extract.Extractor
	enricher  *enrichment.Enricher
	loader    *load.Loader
}

// NewPipeline creates a new Pipeline instance
func NewPipeline(storageClient *storage_go.Client, openaiClient openai.Client, bucketName string) *Pipeline {
	return &Pipeline{
		extractor: extract.NewExtractor(storageClient, bucketName),
		enricher:  enrichment.NewEnricher(openaiClient),
		loader:    load.NewLoader(storageClient, bucketName),
	}
}

// Run executes the complete ETL pipeline
func (p *Pipeline) Run(enableCleanup bool) error {
	log.Println("Starting ETL pipeline...")

	// Extract phase
	log.Println("Phase 1: Extracting data...")
	start := time.Now()
	items, err := p.extractor.Extract()
	if err != nil {
		return err
	}
	log.Printf("Extracted %d batches in %v", len(items), time.Since(start))

	// Enrichment phase
	log.Println("Phase 2: Enriching data with AI...")
	start = time.Now()
	// For demonstration, processing only first 4 batches
	if len(items) > 4 {
		items = items[:4]
	}
	p.enricher.Execution(items)
	log.Printf("AI processing completed in %v", time.Since(start))

	// Load phase
	log.Println("Phase 3: Loading processed data...")
	start = time.Now()
	filesData := p.extractor.GetFilesData()
	results := p.enricher.GetResults()

	if err := p.loader.Load(filesData, results); err != nil {
		return err
	}
	log.Printf("Data loading completed in %v", time.Since(start))

	// Cleanup phase (optional)
	if enableCleanup {
		log.Println("Phase 4: Cleaning up processed files...")
		filesName := p.extractor.GetFilesName()
		fileState := p.extractor.GetFileState()
		if err := p.loader.DeleteProcessedFiles(&filesName, &fileState); err != nil {
			log.Printf("Warning: Cleanup failed: %v", err)
		}
	}

	log.Println("ETL pipeline completed successfully!")
	return nil
}

// initializeClients sets up the required client connections
func initializeClients() (*storage_go.Client, openai.Client, error) {
	// Load environment variables
	if err := godotenv.Load("/app/.env"); err != nil {
		_ = godotenv.Load("/home/aymen/Desktop/my_work/data_engineer/.env")
	}

	// Initialize Supabase storage client
	projectURL := os.Getenv("project_url") + "/storage/v1"
	projectKey := os.Getenv("project_jwt_key")
	if projectURL == "" || projectKey == "" {
		return nil, openai.Client{}, log.Output(1, "Missing Supabase credentials")
	}
	storageClient := storage_go.NewClient(projectURL, projectKey, nil)

	// Initialize OpenAI client (for llama.cpp)
	llamaCPPBaseURL := os.Getenv("LLAMA_CPP_BASE_URL")
	if llamaCPPBaseURL == "" {
		llamaCPPBaseURL = "http://localhost:8000/v1"
	}
	openaiClient := openai.NewClient(
		option.WithBaseURL(llamaCPPBaseURL),
		option.WithAPIKey("dummy"), // llama.cpp doesn't require a real API key
	)

	return storageClient, openaiClient, nil
}

func main() {
	// Initialize clients
	storageClient, openaiClient, err := initializeClients()
	if err != nil {
		log.Fatalf("Failed to initialize clients: %v", err)
	}

	// Create and run pipeline
	pipeline := NewPipeline(storageClient, openaiClient, "datalake")

	// Set to true to enable automatic cleanup of processed files
	enableCleanup := false

	if err := pipeline.Run(enableCleanup); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}
}
