# Use the official Go image as the base image
FROM golang:1.25-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Install necessary packages for building
RUN apk add --no-cache git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/etl-pipeline ./cmd/main.go

# Use a minimal alpine image for the final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create a non-root user
RUN addgroup -g 1001 -S appuser && \
    adduser -S -D -u 1001 -G appuser appuser

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/bin/etl-pipeline .

# Create data directory for parquet files
RUN mkdir -p /app/data

# Change ownership to appuser
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port (if needed for health checks or metrics)
EXPOSE 8080

# Command to run the application
CMD ["./etl-pipeline"]
