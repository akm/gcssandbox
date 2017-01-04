// Sample storage_quickstart creates a Google Cloud Storage bucket.
package main

import (
	"fmt"
	"golang.org/x/net/context"
	"log"
	"time"
	"os"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

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
	topicName := os.Getenv("TOPIC")

	// Creates a storageClient
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storageClient: %v", err)
	}

	// Creates a datastoreClient
	datastoreClient, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create datastoreClient for %s: %v", projectID, err)
	}

	// Creates a pubsubClient
	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create pubsubClient for %s: %v", projectID, err)
	}

	topic := pubsubClient.Topic(topicName)

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
			} else {
				attrs := map[string]string {
					"download_files": url,
				}
				msgIDs, err := topic.Publish(ctx, &pubsub.Message{
					Attributes: attrs,
				})
				if err != nil {
					log.Fatalln("Failed to publish of insertion of ", url, " cause of ", err)
				} else {
					fmt.Println("Message[", msgIDs, "] is published successfully")
				}
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
