package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/container"
	"cloud.google.com/go/monitoring/apiv3"
	"cloud.google.com/go/storage"
	"github.com/golang/protobuf/ptypes"
	"github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"google.golang.org/api/cloudresourcemanager/v1"
	apiiterator "google.golang.org/api/iterator"
	"google.golang.org/api/option"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
	"google.golang.org/grpc"
)

func ListGCEProjects() ([]*cloudresourcemanager.Project, error) {
	client := oauth2.NewClient(context.Background(), nil)
	service, err := cloudresourcemanager.New(client)
	if err != nil {
		return nil, errors.New("Unable to create resource manager client: " + err.Error())
	}

	response, err := service.Projects.List().Do()
	if err != nil {
		return nil, errors.New("Unable to list projects: " + err.Error())
	}

	return response.Projects, nil
}

func listGKEClusters(projectName string) ([]string, error) {
	containerSvc, err := container.NewClient(context.Background(), projectName)
	if err != nil {
		return nil, fmt.Errorf("Failed to create container client: %s", err.Error())
	}

	clusters, err := containerSvc.Clusters(context.Background(), "")
	if err != nil {
		return nil, fmt.Errorf("Failed to list container clusters: %s", err.Error())
	}

	clusterNames := []string{}
	for _, cluster := range clusters {
		clusterNames = append(clusterNames, cluster.Name)
	}

	return clusterNames, nil
}

func ProcessMetrics(
	metricType string,
	projectName string,
	metrics []string,
	client *monitoring.MetricClient,
	interval monitoringpb.TimeInterval,
	bucket *storage.BucketHandle) error {
	maxReceiveSizeOption := gax.WithGRPCOptions(grpc.MaxCallRecvMsgSize(12000000))
	for _, metric := range metrics {
		tempDir, err := ioutil.TempDir("/tmp", "metrics")
		if err != nil {
			return errors.New("Unable to create temp dir: " + err.Error())
		}
		timeIterator := client.ListTimeSeries(context.Background(), &monitoringpb.ListTimeSeriesRequest{
			Name:     "projects/" + projectName,
			Filter:   "metric.type=\"" + metric + "\"",
			Interval: &interval,
		}, maxReceiveSizeOption)

		fmt.Println("Reading metric " + metric)
		t, err := timeIterator.Next()
		if err == apiiterator.Done {
			fmt.Println("No metrics")
		} else if err != nil {
			return fmt.Errorf("Unable to list time series: " + err.Error())
		}

		objectCount := 1
		count := 1
		currentStagingSize := 0
		// Store up to 50mb files
		thresholdSize := 1024 * 1024 * 50
		normalizedMetricName := strings.Replace(metric, "/", "_", -1)
		for err == nil {
			tJson, _ := json.Marshal(t)
			currentStagingSize += len(tJson)
			fileName := tempDir + normalizedMetricName + "-" + strconv.Itoa(count)
			err = ioutil.WriteFile(fileName, tJson, 0644)
			if currentStagingSize >= thresholdSize {
				objectName := metricType + "-" + normalizedMetricName + strconv.Itoa(objectCount)
				tarFile := "/tmp/" + objectName
				cmd := exec.Command("tar", "-czf", tarFile, tempDir)
				cmd.Stderr = os.Stderr
				cmd.Stdout = os.Stdout
				if err := cmd.Run(); err != nil {
					return errors.New("Unable to tar staged metrics: " + err.Error())
				}

				objectBytes, err := ioutil.ReadFile(tarFile)
				if err != nil {
					return fmt.Errorf("Unable to read tarred file %s: %s", tarFile, err.Error())
				}
				_, err = bucket.Object(objectName).NewWriter(context.Background()).Write(objectBytes)
				if err != nil {
					return fmt.Errorf("Unable to write metric to object %s: %s", objectName, err.Error())
				}

				if err := os.RemoveAll(tarFile); err != nil {
					return fmt.Errorf("Unable to delete tar file after upload: %s", err.Error())
				}

				if err := os.RemoveAll(tempDir); err != nil {
					return fmt.Errorf("Unable to delete temp dir %s: %s", tempDir, err.Error())
				}

				tempDir, err = ioutil.TempDir("/tmp", "metrics")
				if err != nil {
					return fmt.Errorf("Unable to create tempr dir: " + err.Error())
				}

				currentStagingSize = 0
				objectCount += 1
			}

			t, err = timeIterator.Next()
			count += 1
		}
	}

	return nil
}

func DownloadMetrics(company string, projectName string, hyperpilotServiceAccountFile string, serviceAccountFile string) error {
	client, err := monitoring.NewMetricClient(context.Background(), option.WithCredentialsFile(serviceAccountFile))
	if err != nil {
		return fmt.Errorf("Failed to create monitoring client: " + err.Error())
	}

	req := &monitoringpb.ListMetricDescriptorsRequest{
		Name: "projects/" + projectName,
	}

	iterator := client.ListMetricDescriptors(context.Background(), req)
	descriptor, err := iterator.Next()
	if err != nil {
		return fmt.Errorf("Unable to get first metric descriptor: " + err.Error())
	}

	computeMetricNames := []string{}
	containerMetricNames := []string{}
	for err == nil {
		if strings.Contains(descriptor.Name, "compute.googleapis.com/instance") {
			fmt.Println("Compute Metric found: ", descriptor.Type)
			computeMetricNames = append(computeMetricNames, descriptor.Type)
		} else if strings.Contains(descriptor.Name, "container.googleapis.com/container") {
			fmt.Println("Container metric found: " + descriptor.Type)
			containerMetricNames = append(containerMetricNames, descriptor.Type)
		}

		descriptor, err = iterator.Next()
	}

	endTime, err := ptypes.TimestampProto(time.Now())
	if err != nil {
		return fmt.Errorf("Unable to parse endtime: " + err.Error())
	}

	// Google saves stackdriver metrics up to 6 weeks.
	startTime, err := ptypes.TimestampProto(time.Now().AddDate(0, -2, 0))
	if err != nil {
		return fmt.Errorf("Unable to parse starttime: " + err.Error())
	}

	interval := monitoringpb.TimeInterval{
		EndTime:   endTime,
		StartTime: startTime,
	}

	fmt.Println("Pulling metrics with interval ", interval)

	storageClient, err := storage.NewClient(context.Background(), option.WithCredentialsFile(hyperpilotServiceAccountFile))
	if err != nil {
		return fmt.Errorf("Unable to create storage client: " + err.Error())
	}

	bucketAttrs := &storage.BucketAttrs{}
	companyName := strings.ToLower(company)
	bucket := storageClient.Bucket("stackdriver-" + companyName + "-" + projectName)
	err = bucket.Create(context.Background(), "gke-data-exporter", bucketAttrs)
	if err != nil {
		return errors.New("Unable to create new bucket: " + err.Error())
	}

	if err := ProcessMetrics("compute", projectName, computeMetricNames, client, interval, bucket); err != nil {
		return errors.New("Unable to process compute metrics: " + err.Error())
	}

	if err := ProcessMetrics("container", projectName, containerMetricNames, client, interval, bucket); err != nil {
		return errors.New("Unable to process compute metrics: " + err.Error())
	}

	return nil
}

func main() {
	listProjects := flag.Bool("list-projects", false, "List GCE projects")
	projectsString := flag.String("projects", "", "GCE projects to download metrics")
	serviceAccountFile := flag.String("service-account", "", "Path to service account file")
	hyperpilotServiceAccountFile := flag.String("hyperpilot-service-account", "", "Path to hyperpilot service account file")
	company := flag.String("company", "hyperpilot", "Company name")
	flag.Parse()

	if *listProjects {
		projects, err := ListGCEProjects()
		if err != nil {
			fmt.Println("Unable to list gce projects: " + err.Error())
			return
		}

		fmt.Println("List projects:")
		for _, project := range projects {
			fmt.Println("- " + project.Name)
		}

		return
	}

	projects := strings.Split(*projectsString, ",")
	if len(projects) == 0 {
		fmt.Println("No projects found")
		return
	}

	if *serviceAccountFile == "" {
		fmt.Println("No service account path found")
		return
	}

	for _, project := range projects {
		if err := DownloadMetrics(*company, strings.TrimSpace(project), *hyperpilotServiceAccountFile, *serviceAccountFile); err != nil {
			fmt.Printf("Unable to download metrics for project %s: " + err.Error())
			return
		}
	}
}
