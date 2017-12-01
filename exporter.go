package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/container"
	"cloud.google.com/go/storage"
	"cloud.google.com/go/monitoring/apiv3"
	"github.com/golang/protobuf/ptypes"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	apiiterator "google.golang.org/api/iterator"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
	"google.golang.org/grpc"
)

func ListGCEProjects() {

}

func listGKEClusters(projectName string) error {
	containerSvc, err := container.NewClient(context.Background(), projectName)
	if err != nil {
		return fmt.Errorf("Failed to create container client: %s", err.Error())
	}

	clusters, err := containerSvc.Clusters(context.Background(), "")
	if err != nil {
		return fmt.Errorf("Failed to list container clusters: %s", err.Error())
	}

	for _, cluster := range clusters {
		fmt.Println("Clusters: %+v", cluster)
	}

	return nil
}

func StoreMetrics(client *storage.Client) error {
	client.Bucket("stackdriver-metrics")
}

func main() {
	namespaces := []string{"compute.googleapis.com/instance", "container.googleapis.com/container"}
	projectName := "hyperpilot-shared-gcp"



	client, err := monitoring.NewMetricClient(context.Background())
	if err != nil {
		fmt.Println("Failed to create monitoring client")
		panic(err)
	}

	req := &monitoringpb.ListMetricDescriptorsRequest{
		Name: "projects/" + projectName,
	}

	iterator := client.ListMetricDescriptors(context.Background(), req)
	descriptor, err := iterator.Next()
	if err != nil {
		fmt.Println("Unable to get first metric")
		panic(err)
	}

	metricNames := []string{}
	for err == nil {
		for _, namespace := range namespaces {
			if strings.Contains(descriptor.Name, namespace) {
				fmt.Println("Metric found: ", descriptor.Type)
				metricNames = append(metricNames, descriptor.Type)
				break
			}
		}

		descriptor, err = iterator.Next()
	}

	endTime, err := ptypes.TimestampProto(time.Now())
	if err != nil {
		fmt.Println("Unable to parse endtime")
		panic(err)
	}

	startTime, err := ptypes.TimestampProto(time.Now().AddDate(0, -2, 0))
	if err != nil {
		fmt.Println("Unable to parse starttime")
		panic(err)
	}

	storageClient, err := storage.NewClient(context.Background())
	if err != nil {
		panic(err)
	}

	interval := monitoringpb.TimeInterval{
		EndTime:   endTime,
		StartTime: startTime,
	}
	fmt.Println("Interval ", interval)
	maxReceiveSizeOption := gax.WithGRPCOptions(grpc.MaxCallRecvMsgSize(6000000))
	for _, metric := range metricNames {
		timeIterator := client.ListTimeSeries(context.Background(), &monitoringpb.ListTimeSeriesRequest{
			Name:     "projects/" + projectName,
			Filter:   "metric.type=\"" + metric + "\"",
			Interval: &interval,
		}, maxReceiveSizeOption)

		fmt.Println("Loading for " + metric)
		t, err := timeIterator.Next()
		if err == apiiterator.Done {
			fmt.Println("No metrics")
		} else if err != nil {
			fmt.Println("Unable to list time series")
			panic(err)
		}

		count := 1
		for err == nil {
			tJson, _ := json.Marshal(t)
			fileName := "/tmp/metrics/" + strings.Replace(metric, "/", "_", -1) + strconv.Itoa(count)
			fmt.Println("Writing file " + fileName)
			err = ioutil.WriteFile(fileName, tJson, 0644)
			if err != nil {
				fmt.Println("Unable to write file " + fileName)
				panic(err)
			}
			StoreMetrics(storageClient)
			t, err = timeIterator.Next()
			count += 1
		}
	}
}
