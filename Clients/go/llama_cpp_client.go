package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	"os"

	openai "github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/option"
	"github.com/openai/openai-go/v2/shared"
	"github.com/parquet-go/parquet-go"

	"encoding/json"

	"github.com/invopop/jsonschema"
	storage_go "github.com/supabase-community/storage-go"
)

// The URL of your running llama.cpp server.
// It must include the /v1 suffix to be compatible with the OpenAI API.





// define types

type message struct {
	Role string
	Content string
}

type Item struct {
	ItemID      int `json:"item_id"`
	Description string `json:"description"`
}

type Product struct {
	ProductName        string  `json:"product_name"`
	Price              float32 `json:"price"`
	Quantity           int     `json:"quantity"`
	Category           string  `json:"category"`
	Description        string  `json:"description"`
	Availability       bool    `json:"availability"`
	DiscountPercentage float32 `json:"discount_percentage"`
	Date               string  `json:"date"`
	ItemID             int     `json:"item_id"`
}


type ItemReview struct {
	ItemID        int    `json:"item_id" jsonschema:"title=item_id,description=unique identifier that is provided in the input."`
	Classification string `json:"classification" jsonschema:"title=Classification,description=The classification of the item (e.g.,Food,cloths)."`
	Review         string `json:"review" jsonschema:"title=Review,description=A brief review of the item."`
}

// Response corresponds to the root schema
type Response struct {
	Reviews []ItemReview `json:"reviews" jsonschema:"title=Reviews,description=A list of classifications and reviews for the provided items.,minItems=25,maxItems=25"`
}

type MergedResponse struct {
	ProductName        string  `json:"product_name"`
	Price              float32 `json:"price"`
	Quantity           int     `json:"quantity"`
	Category           string  `json:"category"`
	Description        string  `json:"description"`
	Availability       bool    `json:"availability"`
	DiscountPercentage float32 `json:"discount_percentage"`
	Date               string  `json:"date"`
	ItemID             int     `json:"item_id"`
	Classification     string  `json:"classification"`
	Review             string  `json:"review"`
	
}

// Global shared variables 

var (
	resultsMu sync.Mutex
	fileMu sync.Mutex
	filesdata []Product
	results   []Response
	logMu     sync.Mutex
)

const BATCHSIZE = 25
// read parquet file


func GenerateSchema[T any]() interface{} {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties: false,
		DoNotReference:            true,
	}
	var v T
	schema := reflector.Reflect(v)
	return schema
}


func transform[T any](data []T,batchsize int ) [][]T {
	var transformed [][]T
	for i := 0; i < len(data); i += batchsize {
		end := i + batchsize
		if end > len(data) {
			end = len(data)
		}
		transformed = append(transformed, data[i:end])
	}
	return transformed
}

func read_parquet(file_path string) [][]Item {
	// Implement reading from a parquet file
	rows, err := parquet.ReadFile[Product](file_path)
	if err != nil {
		log.Fatalf("Failed to read parquet file: %v", err)
	}
	var items []Item
	var i int = 1
	for _, p := range rows {
		items = append(items, Item{
			ItemID:      i,
			Description: p.Description,
		})
		i++
	}

	var batches [][]Item = transform(items, BATCHSIZE)


	return batches
}


// create prompt 

func dict_to_text(batch_items []Item) string {
	var text string
	for _, item := range batch_items {
		text += fmt.Sprintf("Item ID: %d\nDescription: %s\n\n", item.ItemID, item.Description)
	}
	return text
}

func creatPrompt(batch_items []Item ) []message {
	prompt_instruction := `
			You are a helpful assistant that classifies and reviews items.
			
			Each item has:
			- "item_id": unique id for each item
			- "description": the item's description
			
			Return a JSON array of objects with the following keys:
			- "item_id" : same as item_id from input
			- "category" : classification of item category  
			- "review" : small review 1-2 phrases max
			`

    numberOfItems := len(batch_items)
	items := dict_to_text(batch_items) //create this function
	usermessage := fmt.Sprintf("The following %d items need to be classified and reviewed:\n\n%s", numberOfItems, items)
    // --- Create the slice of ChatCompletionMessage structs ---
    // This is the Go equivalent of your Python list of dictionaries.
	context := []message{{Role: "system", Content: prompt_instruction}, {Role: "user", Content: usermessage}}
	return context
}



// A function to handle a single request with retries


//generate schema
func getSchema() openai.ChatCompletionNewParamsResponseFormatUnion {
	schema := GenerateSchema[Response]()
	schemaParam := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:        "Items schema",
		Description: openai.String("genrated reviews and item classficiation"),
		Schema:      schema,
		Strict:      openai.Bool(true),
	}
	return openai.ChatCompletionNewParamsResponseFormatUnion{
		OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{
			JSONSchema: schemaParam,
		},
	}

}

// handleRequest
func handleRequest(ctx context.Context, client openai.Client, prompt []message,process_id int) {
	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		log.Printf("Sending request, attempt %d\n", i+1)

		// Create the chat completion request
		chatCompletion, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
			Messages: []openai.ChatCompletionMessageParamUnion{
				openai.SystemMessage(prompt[0].Content),
				openai.UserMessage(prompt[1].Content),
			},
			Model: shared.ChatModelGPT5,  // You can specify a model, but llama.cpp often ignores this
			ResponseFormat: getSchema(),
			Temperature:    openai.Float(1),
			
		})

		if err == nil && len(chatCompletion.Choices) > 0 {
			// Response is valid, append to results list
			resultsMu.Lock()
			var response Response
			_ = json.Unmarshal([]byte(chatCompletion.Choices[0].Message.Content), &response)
			results = append(results, response)
			resultsMu.Unlock()
			log.Printf("Request successful, data appended.\n")
			return // Success, exit goroutine
		}

		// Response is not valid or an error occurred, retry
		log.Printf("Request failed: %v, retrying...\n", err)
		time.Sleep(3 * time.Second) // Wait before retrying
	}

	// Max retries reached, log the error
	logError("Max retries exceeded")
}


//instead off appending to array create a map that have process_id : {responce : , batch_id}

func execution(data [][]Item, client openai.Client) {
	numRequests := 5

	for i:=0; i<len(data); i+=numRequests {
		var wg sync.WaitGroup
		end := i + numRequests
		
		if end > len(data) {
			end = len(data)
		}

		var prompts [][]message
		for j := i; j < end; j++ {
			prompts = append(prompts, creatPrompt(data[j]))
		}

		for j := 0; j < numRequests; j++ {
			wg.Add(1)
			go func(p []message) {
				defer wg.Done()
				// Pass a context for cancellation/timeouts
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				handleRequest(ctx, client, p,j)
			}(prompts[j])
		}
		wg.Wait()
	}

}


// Logs errors to a file (simulated)
func logError(errMsg string) {
	logMu.Lock()
	log.Printf("Error: %s\n", errMsg)
	// In a real application, you would write this to a file
	logMu.Unlock()
}

//create a supdabase client
// list all files in bucket

func listFilesInBucket(client *storage_go.Client, bucketName string) ([]string, error) {

	files, err := client.ListFiles(bucketName,"crawler1", storage_go.FileSearchOptions{
		SortByOptions: storage_go.SortBy{
			Column: "created_at",
			Order:  "asc",
		},
	})
	
	if err != nil {
		return nil, err
	}

	var fileNames []string
	for _, file := range files {
		fileName := fmt.Sprintf("%s/%s", bucketName, file.Name)
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil
}
// using go routine download the files

func handleFileDownload(client *storage_go.Client, fileName string, wg *sync.WaitGroup) {
			defer wg.Done()
			result, err := client.DownloadFile("bucket-id", fileName)
			if err != nil {
				logError(err.Error())
			} else {
				fileMu.Lock()
				var product Product
				err = json.Unmarshal(result, &product)
				if err != nil {
					logError(err.Error())
				} else {
					filesdata = append(filesdata, product)
				}
				fileMu.Unlock()
				logError("successfully downloaded file: " + fileName)
			}
		}

func downloadFile(client *storage_go.Client, bucketName string, fileNames []string) error {
	var wg sync.WaitGroup
	for ind, fileName := range fileNames {
		wg.Add(1)
		go handleFileDownload(client, fileName, &wg)
		if ind%5 == 0 {
			wg.Wait() // Wait for current batch to finish
		}
	}
	return nil
}

// process the files and extract the data

func processFilesData(data []Product) [][]Item {
	var items [][]Item
	var elements []Item

	for _, product := range data {
		item := Item{
			ItemID:      int(product.ItemID),
			Description: product.Description,
		}
		elements = append(elements, item)
	}
	items = transform(elements, BATCHSIZE)
	return items
}

//the extract function that read execute all the commands

func extract(client *storage_go.Client, bucketName string) [][]Item {
	fileNames, err := listFilesInBucket(client, bucketName)
	if err != nil {
		logError(err.Error())
		return nil
	}
	err = downloadFile(client, bucketName, fileNames)
	if err != nil {
		logError(err.Error())
		return nil
	}
	items := processFilesData(filesdata)
	return items
}

//function that merge both of the response and the data based on item id
func mergeResponses() []MergedResponse {
	var merged []MergedResponse
	var resultsList map[int]ItemReview = make(map[int]ItemReview)
	// Create a map for quick lookup of reviews by ItemID
	for _, response := range results {
		for _, review := range response.Reviews {
			resultsList[review.ItemID] = review
		}
	}


	for _, product := range filesdata {
		if review, exists := resultsList[product.ItemID]; exists {
			merged = append(merged, MergedResponse{
						ProductName:        product.ProductName,
						Price:              product.Price,
						Quantity:           product.Quantity,
						Category:           product.Category,
						Description:        product.Description,
						Availability:       product.Availability,
						DiscountPercentage: product.DiscountPercentage,
						Date:               product.Date,
						ItemID:             product.ItemID,
						Classification:     review.Classification,
						Review:             review.Review,
					})
		}
	}
	return merged
}


func uploadBatch(client *storage_go.Client, bucketName string, batch []MergedResponse ,wg *sync.WaitGroup) {
			defer wg.Done()
			bites,err:= json.Marshal(batch)
			if err != nil {
				logError(err.Error())
			} else 
			{
				contentType := "application/json"
				upsert := true
				client.UploadFile(bucketName, fmt.Sprintf("processed/processed_data_%d.json", time.Now().Unix()), bytes.NewReader(bites), storage_go.FileOptions{
					ContentType:  &contentType,
					Upsert:       &upsert,
				})
				
			logError("successfully uploaded file: " + fmt.Sprintf("processed_data_%d.json", time.Now().Unix()))
			}
				
		}

// create the load function that load the data in supdabase silver bucket in go routine
func UploadFile(client *storage_go.Client, bucketName string, batchdata [][]MergedResponse) {
	var wg sync.WaitGroup
	for ind, batch := range batchdata {
		wg.Add(1)
		go uploadBatch(client, bucketName, batch,&wg)
		if ind%5 == 0 {
			wg.Wait() 
		}
	}
}

func load(client *storage_go.Client, bucketName string) {
	mergedData := mergeResponses()
	var batchdata [][]MergedResponse = transform(mergedData, 50000)
	UploadFile(client, bucketName, batchdata)
}

func main() {

	// Initialize the go-openai client with the llama.cpp server URL
	// The API key is often a dummy value for local servers
	project_id := os.Getenv("project_url")
	project_key := os.Getenv("project_key")
	storageclient := storage_go.NewClient(project_id, project_key, nil)


	llamaCPPBaseURL := os.Getenv("LLAMA_CPP_BASE_URL")
	if llamaCPPBaseURL == "" {
		llamaCPPBaseURL = "http://localhost:8000/v1"
	}
	client := openai.NewClient(
		option.WithBaseURL(llamaCPPBaseURL),
		option.WithAPIKey("dummy"),
	)

	//read parquet
	//read from supabase
	var data [][]Item = extract(storageclient, "datalake")
	
	start := time.Now()	
	execution(data, client)
	log.Printf("Processing time: %v\n", time.Since(start))
	load(storageclient, "datalake")
	log.Printf("Process completed successfully.\ndata stored in processed folder")
	
}