package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	// MinIO connection parameters
	endpoint := flag.String("endpoint", "localhost:9000", "MinIO server endpoint")
	accessKeyID := flag.String("access-key", "myaccesskey", "Access key ID for MinIO")
	secretAccessKey := flag.String("secret-key", "mysecretkey", "Secret access key for MinIO")
	useSSL := flag.Bool("use-ssl", false, "Use SSL for MinIO connection")
	bucketName := flag.String("bucket", "sleeve", "Bucket name to fetch objects from")
	outputFile := flag.String("output-file", "trace.jsonl", "Output file to save the objects")
	cleanup := flag.Bool("cleanup", false, "Empty the bucket after fetching objects")

	// Parse flags
	flag.Parse()

	// Initialize MinIO client
	minioClient, err := minio.New(*endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(*accessKeyID, *secretAccessKey, ""),
		Secure: *useSSL,
	})
	if err != nil {
		log.Fatalf("Failed to initialize MinIO client: %v", err)
	}

	// Create output file
	file, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Channel for objects
	objectsCh := make(chan minio.ObjectInfo, 100)

	// WaitGroup to track completion
	var wg sync.WaitGroup

	// Start multiple workers to download objects
	workerCount := 10
	wg.Add(workerCount)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	// Process objects
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()

			for object := range objectsCh {
				// Get object
				obj, err := minioClient.GetObject(ctx, *bucketName, object.Key, minio.GetObjectOptions{})
				if err != nil {
					log.Printf("Error getting object %s: %v", object.Key, err)
					continue
				}

				// Read and write the object content
				content, err := io.ReadAll(obj)
				if err != nil {
					log.Printf("Error reading object %s: %v", object.Key, err)
					continue
				}

				// Add data to output file with mutex to prevent interleaved writes
				mutex.Lock()
				writer.Write(content)
				writer.WriteString("\n") // Add newline for JSONL format
				mutex.Unlock()

				log.Printf("Processed object: %s", object.Key)
			}
		}()
	}

	// List all objects recursively
	log.Println("Starting to list objects...")

	// This recursive listing will find all objects in all "folders"
	objectCh := minioClient.ListObjects(ctx, *bucketName, minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    "", // No prefix means all objects
	})

	// Send objects to channel
	objectCount := 0
	for object := range objectCh {
		if object.Err != nil {
			log.Printf("Error listing: %v", object.Err)
			continue
		}

		objectCount++
		objectsCh <- object
	}

	close(objectsCh)
	log.Printf("Found %d objects. Waiting for processing to complete...", objectCount)

	// Wait for all downloads to complete
	wg.Wait()

	log.Printf("All %d objects processed. Output saved to %s", objectCount, *outputFile)
	if *cleanup {
		log.Println("Starting cleanup...")

		objectsCh := minioClient.ListObjects(ctx, *bucketName, minio.ListObjectsOptions{
			Recursive: true,
		})

		// Remove the objects in the bucket using the RemoveObjects API
		for rErr := range minioClient.RemoveObjects(ctx, *bucketName, objectsCh, minio.RemoveObjectsOptions{}) {
			if rErr.Err != nil {
				log.Printf("error removing object %s: %v", rErr.ObjectName, rErr.Err)
			}
		}

		// return nil
	}
}

// Global mutex for file writing
var mutex sync.Mutex
