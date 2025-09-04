package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	openai "github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/option"
	"github.com/openai/openai-go/v2/shared"
	"github.com/parquet-go/parquet-go"

	"encoding/json"

	"github.com/invopop/jsonschema"
)

// The URL of your running llama.cpp server.
// It must include the /v1 suffix to be compatible with the OpenAI API.
const llamaCPPBaseURL = "http://localhost:8080/v1"


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
	ProductName        string  `parquet:"product_name"`
	Price              float64 `parquet:"price"`
	Quantity           int64   `parquet:"quantity"`
	Category           string  `parquet:"category"`
	Description        string  `parquet:"description"`
	Availability       bool    `parquet:"availability"`
	DiscountPercentage float64 `parquet:"discount_percentage"`
	Date               string  `parquet:"date"`
	ShopID             string  `parquet:"shop_id"`
	ID                 string  `parquet:"id"`
	ItemID             int64   `parquet:"item_id"`
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


// Global shared variables   

var (
	resultsMu sync.Mutex
	results   map[int]Response = make(map[int]Response)
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


func transform(data []Item,batchsize int ) [][]Item {
	var transformed [][]Item
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
			results[process_id] = response
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
		break
	}

}


// Logs errors to a file (simulated)
func logError(errMsg string) {
	logMu.Lock()
	log.Printf("Error: %s\n", errMsg)
	// In a real application, you would write this to a file
	logMu.Unlock()
}

func main() {

	// Initialize the go-openai client with the llama.cpp server URL
	// The API key is often a dummy value for local servers
	
	client := openai.NewClient(
		option.WithBaseURL(llamaCPPBaseURL),
		option.WithAPIKey("dummy"),
	)

	//read parquet
	var file_path string = "../../data/data.parquet"
	var data [][]Item = read_parquet(file_path)
	var parts []Response
	start := time.Now()	
	execution(data, client)
	log.Printf("Processing time: %v\n", time.Since(start))
	for i:=0; i<len(results); i++ {
		parts = append(parts, results[i])
	}
	log.Printf("Final aggregated results: %+v\n\n", parts)

	
}