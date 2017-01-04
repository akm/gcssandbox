// Sample storage_quickstart creates a Google Cloud Storage bucket.
package main

import (
	"fmt"
	"golang.org/x/net/context"
	"log"
	"time"
	"os"

	// Imports the Google Cloud Storage client package
	"cloud.google.com/go/storage"

	// Imports the Google Cloud Datastore client package
	"cloud.google.com/go/datastore"

	"google.golang.org/api/iterator"
)

type Watch struct {
}

type UploadedFile struct {
	Url string `datastore:"url"`
  Updated time.Time  `datastore:"updated"`
}

func main() {
	ctx := context.Background()

	projectID := os.Getenv("PROJECT")
	bucketName := os.Getenv("BUCKET")

	// Creates a storageClient
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storageClient: %v", err)
	}

	// Creates a datastoreClient
	datastoreClient, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create datastoreClient: %v", err)
	}

	watchKey := datastore.NameKey("Watches", "1000001", nil)
	watch := &Watch{}
	if err := datastoreClient.Get(ctx, watchKey, watch); err != nil {
		_, err = datastoreClient.Put(ctx, watchKey, &Watch{})
		if err != nil {
			log.Fatalf("Failed to put watch data: %v", err)
		}
	}

	q := datastore.NewQuery("UploadedFiles").Ancestor(watchKey)
	var res []UploadedFile
	_, err = datastoreClient.GetAll(ctx, q, &res)
	if err != nil {
		log.Fatalf("Failed to get all uploaded files: %v", err)
	}

	storedFiles := make(map[string]time.Time)
	for _, v := range res {
		storedFiles[v.Url] = v.Updated
	}

	// Prepares a new bucket
	bucket := storageClient.Bucket(bucketName)
	it := bucket.Objects(ctx, nil)
	for {
		o, err := it.Next()
		if err != nil && err != iterator.Done {
			log.Fatal(err)
		}
		if err == iterator.Done {
			break
		}
		url := "gs://" + o.Bucket + "/" + o.Name
		if updated, ok := storedFiles[url]; ok {
			if o.Updated.After(updated) {
				fmt.Println(url, " was updated at", updated, " but now it's ", o.Updated)
				k := datastore.NameKey("UploadedFiles", url, watchKey)
				uf := &UploadedFile{Url: url, Updated: o.Updated}
				if _, err := datastoreClient.Put(ctx, k, uf); err != nil {
					fmt.Println("Failed to put ", uf)
				}
			}
			delete(storedFiles, url)
		} else {
			fmt.Println(url, "was inserted")
			k := datastore.NameKey("UploadedFiles", url, watchKey)
			uf := &UploadedFile{Url: url, Updated: o.Updated}
			if _, err := datastoreClient.Put(ctx, k, uf); err != nil {
				fmt.Println("Failed to put ", uf)
			}
		}
		// fmt.Println(o.Created, o.Updated, o.Bucket, o.Name, o.ContentType, o.Size)
	}
	for url, _ := range storedFiles {
		fmt.Println(url, "was deleted")
		k := datastore.NameKey("UploadedFiles", url, watchKey)
		if err := datastoreClient.Delete(ctx, k); err != nil {
			fmt.Println("Failed to delete ", url)
		}

	}
}
