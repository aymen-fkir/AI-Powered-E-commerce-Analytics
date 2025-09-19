package models

// Message represents a chat message with role and content
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Item represents an item with ID and description
type Item struct {
	ItemID      int    `json:"item_id"`
	Description string `json:"description"`
}

// Product represents the raw product data from the crawler
type Product struct {
	ProductName        string  `json:"product_name"`
	Price              float32 `json:"price"`
	Quantity           int     `json:"quantity"`
	Category           string  `json:"category"`
	Description        string  `json:"description"`
	Availability       bool    `json:"availability"`
	DiscountPercentage float32 `json:"discount_percentage"`
	Date               string  `json:"date"`
	UserID             string  `json:"id"`
	ShopID             string  `json:"shop_id"`
}

// ItemReview represents an AI-generated review for an item
type ItemReview struct {
	ItemID         int    `json:"item_id" jsonschema:"title=item_id,description=unique identifier that is provided in the input."`
	Classification string `json:"classification" jsonschema:"title=Classification,description=The classification of the item (e.g.,Food,cloths)."`
	Review         string `json:"review" jsonschema:"title=Review,description=A brief review of the item."`
}

// Response represents the AI model response containing multiple reviews
type Response struct {
	Reviews []ItemReview `json:"reviews" jsonschema:"title=Reviews,description=A list of classifications and reviews for the provided items.,minItems=25,maxItems=25"`
}

// MergedResponse represents the final merged data combining product info and AI-generated reviews
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
	UserID             string  `json:"id"`
	ShopID             string  `json:"shop_id"`
}
