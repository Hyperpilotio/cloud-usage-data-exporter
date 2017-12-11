package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"cloud.google.com/go/storage"
	apiiterator "google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func DownloadMetric(bucket *storage.BucketHandle, objectName string, tempDir string) (string, error) {
	if !strings.HasSuffix(objectName, ".tar.gz") {
		return "", errors.New("Unexpected suffix from object: " + objectName)
	}

	reader, err := bucket.Object(objectName).NewReader(context.Background())
	if err != nil {
		return "", fmt.Errorf("Unable to read object %s: %s", objectName, err.Error())
	}

	defer reader.Close()

	fileName := tempDir + "/" + objectName
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 644)
	if err != nil {
		return "", fmt.Errorf("Unable to open file %s: %s", fileName, err.Error())
	}

	defer f.Close()

	defaultStagingSize := int64(1024 * 1024 * 5)
	staging := make([]byte, defaultStagingSize)

	for reader.Remain() > 0 {
		currentStaging := staging
		if reader.Remain() < defaultStagingSize {
			currentStaging = make([]byte, reader.Remain())
		}
		_, err := reader.Read(currentStaging)
		if err != nil {
			return "", errors.New("Unable to read from reader: " + err.Error())
		}

		_, err = f.Write(currentStaging)
		if err != nil {
			return "", errors.New("Unable to write staging to file: " + err.Error())
		}
	}

	return fileName, nil
}

func DownloadMetrics(bucketName string, hyperpilotServiceAccountFile string) error {
	storageClient, err := storage.NewClient(context.Background(), option.WithCredentialsFile(hyperpilotServiceAccountFile))
	if err != nil {
		return fmt.Errorf("Unable to create storage client: " + err.Error())
	}

	bucket := storageClient.Bucket(bucketName)
	allObjects := bucket.Objects(context.Background(), &storage.Query{})
	objectAttrs, err := allObjects.Next()
	if err == apiiterator.Done {
		return errors.New("No objects found")
	} else if err != nil {
		return errors.New("Unable to list objects: " + err.Error())
	}

	for err == nil {
		if objectAttrs.Name != "index" {
			tempDir, err := ioutil.TempDir("/tmp", "metric_read")
			if err != nil {
				return errors.New("Unable to create temp dir: " + err.Error())
			}

			fileName, err := DownloadMetric(bucket, objectAttrs.Name, tempDir)
			if err != nil {
				return fmt.Errorf("Unable to download metric %s: % s", objectAttrs.Name, err.Error())
			}

			cmd := exec.Command("gunzip", fileName)
			cmd.Stderr = os.Stderr
			cmd.Stdout = os.Stdout
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("Unable to gunzip file %s: %s", fileName, err.Error())
			}

			fileName = strings.TrimSuffix(fileName, ".gz")

			cmd = exec.Command("tar", "xvf", fileName)
			cmd.Stderr = os.Stderr
			cmd.Stdout = os.Stdout
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("Unable to untar file %s: %s", fileName, err.Error())
			}

			if err := os.Remove(fileName); err != nil {
				return fmt.Errorf("Unable to remove tar file %s: %s", fileName, err.Error())
			}
		}

		objectAttrs, err = allObjects.Next()
	}
}

func main() {
	gsBucket := flag.String("gs-bucket", "", "Google storage bucket name")
	hyperpilotServiceAccountFile := flag.String("hyperpilot-service-account", "", "Hyperpilot service account file")
	flag.Parse()

	if *gsBucket == "" {
		fmt.Println("No gs bucket found")
		return
	}

	if *hyperpilotServiceAccountFile == "" {
		fmt.Println("No hyperpilot service account found")
		return
	}

	if err := DownloadMetrics(*gsBucket, *hyperpilotServiceAccountFile); err != nil {
		fmt.Println("Unable to download metrics: " + err.Error())
		return
	}
}
