package utils

import (
	"log"
	"sync"

	"github.com/invopop/jsonschema"
)

var logMu sync.Mutex

// LogMessage safely logs messages with mutex protection
func LogMessage(msg string) {
	logMu.Lock()
	defer logMu.Unlock()
	log.Printf("%s\n", msg)
}

// Transform splits a slice into batches of specified size
func Transform[T any](data []T, batchSize int) [][]T {
	var transformed [][]T
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		transformed = append(transformed, data[i:end])
	}
	return transformed
}

// GenerateSchema generates JSON schema for a given type
func GenerateSchema[T any]() interface{} {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties: false,
		DoNotReference:            true,
	}
	var v T
	schema := reflector.Reflect(v)
	return schema
}
