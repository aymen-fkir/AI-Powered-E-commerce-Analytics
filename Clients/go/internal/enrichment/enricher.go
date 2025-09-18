package enrichment

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/internal/models"
	"github.com/aymen-fkir/AI-Powered-E-commerce-Analytics/Clients/go/pkg/utils"
	openai "github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/shared"
)

const (
	MaxRetries     = 3
	BatchSize      = 25
	NumRequests    = 4
	RequestTimeout = 60 * time.Second
)

// Enricher handles AI-powered data enrichment
type Enricher struct {
	client    openai.Client
	resultsMu sync.Mutex
	results   []models.Response
}

// NewEnricher creates a new Enricher instance
func NewEnricher(client openai.Client) *Enricher {
	return &Enricher{
		client:  client,
		results: make([]models.Response, 0),
	}
}

// DictToText converts items to text format for prompts
func (e *Enricher) DictToText(batchItems []models.Item) string {
	var text string
	for _, item := range batchItems {
		text += fmt.Sprintf("Item ID: %d\nDescription: %s\n\n", item.ItemID, item.Description)
	}
	return text
}

// CreatePrompt creates prompt messages for AI processing
func (e *Enricher) CreatePrompt(batchItems []models.Item) []models.Message {
	promptInstruction := `
You are a helpful assistant that classifies and reviews items.

Each item has:
- "item_id": unique id for each item
- "description": the item's description

Return a JSON array of objects with the following keys:
- "item_id" : same as item_id from input
- "category" : classification of item category  
- "review" : small review 1-2 phrases max
`

	numberOfItems := len(batchItems)
	items := e.DictToText(batchItems)
	userMessage := fmt.Sprintf("The following %d items need to be classified and reviewed:\n\n%s", numberOfItems, items)

	return []models.Message{
		{Role: "system", Content: promptInstruction},
		{Role: "user", Content: userMessage},
	}
}

// GetSchema generates the JSON schema for the response format
func (e *Enricher) GetSchema() openai.ChatCompletionNewParamsResponseFormatUnion {
	schema := utils.GenerateSchema[models.Response]()
	schemaParam := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:        "Items schema",
		Description: openai.String("generated reviews and item classification"),
		Schema:      schema,
		Strict:      openai.Bool(true),
	}
	return openai.ChatCompletionNewParamsResponseFormatUnion{
		OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{
			JSONSchema: schemaParam,
		},
	}
}

// HandleRequest processes a single AI request with retries
func (e *Enricher) HandleRequest(ctx context.Context, prompt []models.Message) bool {
	for i := 0; i < MaxRetries; i++ {
		utils.LogMessage(fmt.Sprintf("Sending request, attempt %d", i+1))

		chatCompletion, err := e.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
			Messages: []openai.ChatCompletionMessageParamUnion{
				openai.SystemMessage(prompt[0].Content),
				openai.UserMessage(prompt[1].Content),
			},
			Model:          shared.ChatModelGPT5,
			ResponseFormat: e.GetSchema(),
			Temperature:    openai.Float(1),
		})

		if err == nil && len(chatCompletion.Choices) > 0 {
			e.resultsMu.Lock()
			var response models.Response
			if unmarshalErr := json.Unmarshal([]byte(chatCompletion.Choices[0].Message.Content), &response); unmarshalErr == nil {
				e.results = append(e.results, response)
				e.resultsMu.Unlock()
				utils.LogMessage("Request successful, data appended")
				return true
			}
			e.resultsMu.Unlock()
		}

		utils.LogMessage(fmt.Sprintf("Request failed: %v, retrying...", err))
		time.Sleep(3 * time.Second)
	}

	utils.LogMessage("Max retries exceeded")
	return false
}

// ProcessMessageBatch processes a single batch of messages
func (e *Enricher) ProcessMessageBatch(prompt []models.Message, size *int, fileInd *int, wg *sync.WaitGroup) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	if e.HandleRequest(ctx, prompt) {
		*size += BatchSize
		if *size/50000 > 1 {
			*fileInd++
			// Note: fileState would need to be passed or managed differently
		}
	}
}

// Execution orchestrates the AI processing pipeline
func (e *Enricher) Execution(data [][]models.Item) {
	size := 0
	fileInd := 0

	for i := 0; i < len(data); i += NumRequests {
		var wg sync.WaitGroup
		end := i + NumRequests

		if end > len(data) {
			end = len(data)
		}

		var prompts [][]models.Message
		for j := i; j < end; j++ {
			prompts = append(prompts, e.CreatePrompt(data[j]))
		}

		for j := 0; j < len(prompts); j++ {
			wg.Add(1)
			go e.ProcessMessageBatch(prompts[j], &size, &fileInd, &wg)
		}
		wg.Wait()
	}
}

// GetResults returns the AI processing results
func (e *Enricher) GetResults() []models.Response {
	e.resultsMu.Lock()
	defer e.resultsMu.Unlock()
	return e.results
}
