// Sample storage_quickstart creates a Google Cloud Storage bucket.
package main

import (
	"fmt"
	"golang.org/x/net/context"
	"log"
	"os"

	// Imports the Google Cloud Storage client package
	"cloud.google.com/go/storage"

	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()

	// Creates a client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// The name for the new bucket
	bucketName := os.Getenv("BUCKET")

	// Prepares a new bucket
	bucket := client.Bucket(bucketName)

	it := bucket.Objects(ctx, nil)
	for {
		o, err := it.Next()
		if err != nil && err != iterator.Done {
			log.Fatal(err)
		}
		if err == iterator.Done {
			break
		}
		fmt.Println(o.Created, o.Updated, o.Bucket, o.Name, o.ContentType, o.Size)
	}
}
