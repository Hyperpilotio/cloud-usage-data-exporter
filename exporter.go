package main

import (
	"fmt"

	"cloud.google.com/go/container"
	"golang.org/x/oauth2"
)

func main() {
	containerSvc, err := container.NewClient(oauth2.NoContext, "hyperpilot-shared-gcp")
	if err != nil {
		fmt.Println("Failed to create container client")
		panic(err)
	}

	clusters, err := containerSvc.Clusters(oauth2.NoContext, "")
	if err != nil {
		fmt.Println("Failed to list container clusters")
		panic(err)
	}

	for _, cluster := range clusters {
		fmt.Printf("%+v", cluster)
	}
}
