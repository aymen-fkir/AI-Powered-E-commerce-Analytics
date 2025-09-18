package extract

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/internal/models"
	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/pkg/utils"
	storage_go "github.com/supabase-community/storage-go"
)

const BatchSize = 25

// Extractor handles data extraction from storage
type Extractor struct {
	client     *storage_go.Client
	bucketName string
	fileMu     sync.Mutex
	filesData  []models.Product
	fileState  []bool
	filesName  []string
}

// NewExtractor creates a new Extractor instance
func NewExtractor(client *storage_go.Client, bucketName string) *Extractor {
	return &Extractor{
		client:     client,
		bucketName: bucketName,
		filesData:  make([]models.Product, 0),
		fileState:  make([]bool, 0),
		filesName:  make([]string, 0),
	}
}

// ListFilesInBucket lists all files in the specified bucket path
func (e *Extractor) ListFilesInBucket() ([]string, error) {
	files, err := e.client.ListFiles(e.bucketName, "bronze/new/", storage_go.FileSearchOptions{
		SortByOptions: storage_go.SortBy{
			Column: "created_at",
			Order:  "asc",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	var fileNames []string
	for _, file := range files {
		if file.Name == ".emptyFolderPlaceholder" {
			continue
		}
		fileName := fmt.Sprintf("%s/%s", "bronze/new", file.Name)
		e.fileState = append(e.fileState, false)
		e.filesName = append(e.filesName, fileName)
		fileNames = append(fileNames, fileName)
	}
	utils.LogMessage(fmt.Sprintf("Found %d files in bucket", len(fileNames)))
	return fileNames, nil
}

// handleFileDownload downloads and processes a single file
func (e *Extractor) handleFileDownload(fileName string, wg *sync.WaitGroup) {
	defer wg.Done()

	result, err := e.client.DownloadFile(e.bucketName, fileName)
	if err != nil {
		utils.LogMessage(fmt.Sprintf("Error downloading file %s: %v", fileName, err))
		return
	}

	e.fileMu.Lock()
	defer e.fileMu.Unlock()

	var products []models.Product
	if err := json.Unmarshal(result, &products); err != nil {
		utils.LogMessage(fmt.Sprintf("Error unmarshaling file %s: %v", fileName, err))
		return
	}

	e.filesData = append(e.filesData, products...)
}

// DownloadFiles downloads all files concurrently with batch processing
func (e *Extractor) DownloadFiles(fileNames []string) error {
	var wg sync.WaitGroup

	for ind, fileName := range fileNames {
		wg.Add(1)
		go e.handleFileDownload(fileName, &wg)

		// Process in batches of 10 to avoid overwhelming the system
		if ind%10 == 0 {
			wg.Wait()
		}
	}
	wg.Wait()

	utils.LogMessage("Successfully downloaded all files")
	return nil
}

// ProcessFilesData converts product data to items and batches them
func (e *Extractor) ProcessFilesData() [][]models.Item {
	var elements []models.Item

	for ind, product := range e.filesData {
		item := models.Item{
			ItemID:      ind + 1,
			Description: product.Description,
		}
		elements = append(elements, item)
	}

	return utils.Transform(elements, BatchSize)
}

// GetFilesData returns the downloaded files data
func (e *Extractor) GetFilesData() []models.Product {
	return e.filesData
}

// GetFileState returns the file processing state
func (e *Extractor) GetFileState() []bool {
	return e.fileState
}

// GetFilesName returns the file names
func (e *Extractor) GetFilesName() []string {
	return e.filesName
}

// Extract orchestrates the complete extraction process
func (e *Extractor) Extract() ([][]models.Item, error) {
	fileNames, err := e.ListFilesInBucket()
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	if err := e.DownloadFiles(fileNames); err != nil {
		return nil, fmt.Errorf("failed to download files: %w", err)
	}

	items := e.ProcessFilesData()
	return items, nil
}
