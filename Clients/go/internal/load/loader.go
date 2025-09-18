package load

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/internal/models"
	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/pkg/utils"
	storage_go "github.com/supabase-community/storage-go"
)

// Loader handles data loading and storage operations
type Loader struct {
	client     *storage_go.Client
	bucketName string
}

// NewLoader creates a new Loader instance
func NewLoader(client *storage_go.Client, bucketName string) *Loader {
	return &Loader{
		client:     client,
		bucketName: bucketName,
	}
}

// MergeResponses combines product data with AI-generated reviews
func (l *Loader) MergeResponses(filesData []models.Product, results []models.Response) []models.MergedResponse {
	var merged []models.MergedResponse
	resultsList := make(map[int]models.ItemReview)

	// Create a map for quick lookup of reviews by ItemID
	for _, response := range results {
		for _, review := range response.Reviews {
			resultsList[review.ItemID] = review
		}
	}

	// Merge product data with reviews
	for ind, product := range filesData {
		if review, exists := resultsList[ind+1]; exists {
			merged = append(merged, models.MergedResponse{
				ProductName:        product.ProductName,
				Price:              product.Price,
				Quantity:           product.Quantity,
				Category:           product.Category,
				Description:        product.Description,
				Availability:       product.Availability,
				DiscountPercentage: product.DiscountPercentage,
				Date:               product.Date,
				ItemID:             review.ItemID,
				Classification:     review.Classification,
				Review:             review.Review,
			})
		}
	}
	return merged
}

// uploadBatch uploads a single batch of data to storage
func (l *Loader) uploadBatch(batch []models.MergedResponse, wg *sync.WaitGroup) {
	defer wg.Done()

	jsonBytes, err := json.Marshal(batch)
	if err != nil {
		utils.LogMessage(fmt.Sprintf("Error marshaling batch: %v", err))
		return
	}

	contentType := "application/json"
	upsert := true
	filename := fmt.Sprintf("silver/processed_data_%d.json", time.Now().Unix())

	_, err = l.client.UploadFile(l.bucketName, filename, bytes.NewReader(jsonBytes), storage_go.FileOptions{
		ContentType: &contentType,
		Upsert:      &upsert,
	})

	if err != nil {
		utils.LogMessage(fmt.Sprintf("Error uploading file %s: %v", filename, err))
		return
	}

	utils.LogMessage(fmt.Sprintf("Successfully uploaded file: %s", filename))
}

// UploadFile uploads batched data to storage with concurrent processing
func (l *Loader) UploadFile(batchData [][]models.MergedResponse) error {
	var wg sync.WaitGroup

	for ind, batch := range batchData {
		wg.Add(1)
		go l.uploadBatch(batch, &wg)

		// Process in batches of 5 to avoid overwhelming the system
		if ind%5 == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
	return nil
}

// Load orchestrates the complete data loading process
func (l *Loader) Load(filesData []models.Product, results []models.Response) error {
	mergedData := l.MergeResponses(filesData, results)
	batchData := utils.Transform(mergedData, 50000)
	return l.UploadFile(batchData)
}

// DeleteProcessedFiles removes files that have been successfully processed
func (l *Loader) DeleteProcessedFiles(filesName *[]string, fileState *[]bool) error {
	var leftfiles []string
	var leftfilesState []bool
	var nbfilesTomoved int = 0
	for i, fileName := range *filesName {
		if i < len(*fileState) {
			if (*fileState)[i] {
				_, err := l.client.MoveFile(l.bucketName, fileName, "bronze/processed/"+fileName)
				if err != nil {
					return fmt.Errorf("failed to moved files: %w", err)
				}
				nbfilesTomoved++
			} else {
				leftfiles = append(leftfiles, fileName)
				leftfilesState = append(leftfilesState, (*fileState)[i])
			}

		} else {
			utils.LogMessage(fmt.Sprintf("Warning: fileState index %d out of range for filesName", i))
			return fmt.Errorf("fileState index %d out of range for filesName", i)
		}
	}

	if len(leftfiles) == 0 {
		utils.LogMessage("No files to moved")
		// Clear both slices completely
		*filesName = nil
		*fileState = nil
		return nil
	}

	*filesName = make([]string, len(leftfiles))
	*fileState = make([]bool, len(leftfilesState))
	copy(*filesName, leftfiles)
	copy(*fileState, leftfilesState)

	utils.LogMessage(fmt.Sprintf("Successfully moved %d files", nbfilesTomoved))
	return nil
}
