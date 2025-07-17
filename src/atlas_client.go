package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/atlas-sdk/v20250312005/admin"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

var (
	ONE_MINUTE = "PT1M"
	ONE_HOUR   = "PT1H"
	ONE_DAY    = "P1D"
)

type AtlasClient struct {
	HTTPClient *http.Client
	AtlasSDK   *admin.APIClient
}

func NewAtlasClient(sdk *admin.APIClient) *AtlasClient {
	if sdk == nil {
		cfg, err := GetConfig()
		if err != nil {
			Logger.Error("Error getting config")
			panic(err)
		}
		sdk, err = admin.NewClient(admin.UseDigestAuth(cfg.AtlasPublicKey, cfg.AtlasPrivateKey))
	}

	return &AtlasClient{
		AtlasSDK: sdk,
	}
}

func (c *AtlasClient) GetMeasurementsForProcess(ctx context.Context, projectID string, host string, startDate *time.Time, endDate *time.Time, period *string, granularity *string) (*admin.ApiMeasurementsGeneralViewAtlas, error) {
	params := &admin.GetHostMeasurementsApiParams{
		GroupId:     projectID,
		ProcessId:   host,
		Granularity: granularity,
	}
	if period != nil {
		params.Period = period
	} else if startDate != nil && endDate != nil {
		params.Start = startDate
		params.End = endDate
	}
	measurements, response, err := c.AtlasSDK.MonitoringAndLogsApi.GetHostMeasurementsWithParams(ctx, params).Execute()
	if err != nil {
		Logger.Error("Failed to get host metrics: ", err)
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		Logger.Error("Host metrics request not OK")
		return nil, fmt.Errorf("host metrics returned a non-200 response: %d", response.StatusCode)
	}
	return measurements, nil
}

func (c *AtlasClient) GetDisksOnHost(ctx context.Context, projectID string, host string) (*[]admin.MeasurementDiskPartition, error) {
	var result *[]admin.MeasurementDiskPartition
	result = new([]admin.MeasurementDiskPartition)
	hasNextPage := true
	pageNum := 1
	perPage := 100
	includeCount := true

	for hasNextPage == true {
		partitions, response, err := c.AtlasSDK.MonitoringAndLogsApi.ListDiskPartitionsWithParams(ctx, &admin.ListDiskPartitionsApiParams{
			GroupId:      projectID,
			ProcessId:    host,
			PageNum:      &pageNum,
			ItemsPerPage: &perPage,
			IncludeCount: &includeCount,
		}).Execute()
		if err != nil {
			Logger.Error("Failed to get host partitions: ", err)
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			Logger.Error("Host partitions request for host not OK")
			return nil, fmt.Errorf("host partitions returned a non-200 response: %d", response.StatusCode)
		}
		*result = append(*result, *partitions.Results...)
		if len(*result) >= *partitions.TotalCount || len(*partitions.Results) == 0 {
			hasNextPage = false
		} else {
			pageNum++
		}
	}
	return result, nil
}

func (c *AtlasClient) GetAtlasClusterInfo(ctx context.Context, projectID, clusterName string) (*admin.ClusterDescription20240805, error) {
	params := &admin.GetClusterApiParams{
		GroupId:     projectID,
		ClusterName: clusterName,
	}
	desc, response, err := c.AtlasSDK.ClustersApi.GetClusterWithParams(ctx, params).Execute()
	if err != nil {
		Logger.Error("Failed to get host metrics: ", err)
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		Logger.Error("Host metrics request not OK")
		return nil, fmt.Errorf("host metrics returned a non-200 response: %d", response.StatusCode)
	}
	return desc, nil
}

func (c *AtlasClient) GetAtlasClusterInfoString(ctx context.Context, projectID, clusterName string) (string, error) {
	info, err := c.GetAtlasClusterInfo(ctx, projectID, clusterName)
	if err != nil {
		panic(err)
	}
	var specs []string
	for _, rs := range *info.ReplicationSpecs {
		for _, rc := range *rs.RegionConfigs {
			es := rc.ElectableSpecs
			str := fmt.Sprintf("There are %d IOPS available, as the cluster's base Atlas tier is %s.", es.DiskIOPS, *es.InstanceSize)
			specs = append(specs, str)
		}
	}
	return strings.Join(specs, "\n"), nil
}

func (c *AtlasClient) DeleteClusterLogs(ctx context.Context, logFiles []string) error {
	var errs []string
	for _, logFile := range logFiles {
		if err := os.Remove(logFile); err != nil {
			// Log the error and continue, since we want to try deleting all files
			errStr := fmt.Sprintf("failed to delete log file %s: %v", logFile, err)
			Logger.Error(errStr)
			errs = append(errs, errStr)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("encountered errors during log cleanup:\n%s", strings.Join(errs, "\n"))
	}
	fmt.Fprintln(os.Stdout, "Cleaned up temporary files")
	return nil
}

func (c *AtlasClient) DownloadClusterLogs(ctx context.Context, projectID, clusterName string, startDate int64, endDate int64) (map[string]string, error) {
	// TODO: Support sharded clusters!
	Logger.Info("Downloading Atlas cluster logs")
	var hostLogMapping = make(map[string]string)
	info, err := c.GetAtlasClusterInfo(ctx, projectID, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}
	hosts, ports, err := GetHostsFromConnectionString(*info.ConnectionStrings.Standard)
	if err != nil {
		return nil, fmt.Errorf("failed to get hosts from connection string: %w", err)
	}
	var logFiles []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, len(hosts))
	hostLogMappingChan := make(chan struct {
		key     string
		logFile string
	}, len(hosts))

	for i, host := range hosts {
		wg.Add(1)
		go func(i int, host string) {
			defer wg.Done()
			Logger.WithFields(logrus.Fields{"host": host}).Info("Downloading logs for host")
			logFile, err := c.GetClusterLogsForHost(ctx, projectID, host, &startDate, &endDate)
			if err != nil {
				errChan <- fmt.Errorf("failed to download logs for host %s: %w", host, err)
				return
			}
			hostLogMappingChan <- struct {
				key     string
				logFile string
			}{fmt.Sprintf("%s:%s", host, ports[i]), logFile}
			mu.Lock()
			logFiles = append(logFiles, logFile)
			mu.Unlock()
		}(i, host)
	}

	wg.Wait()
	close(errChan)
	close(hostLogMappingChan)

	// Check for errors
	if len(errChan) > 0 {
		_ = c.DeleteClusterLogs(ctx, logFiles)
		return nil, <-errChan
	}

	for entry := range hostLogMappingChan {
		hostLogMapping[entry.key] = entry.logFile
	}
	return hostLogMapping, nil
}

func GetHostsFromConnectionString(connectionString string) ([]string, []string, error) {
	cs, err := connstring.Parse(connectionString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if cs.Scheme == "mongodb+srv" {
		// For SRV records, the host is the only part we need
		return cs.Hosts, nil, nil
	}

	hosts := make([]string, 0, len(cs.Hosts))
	ports := make([]string, 0, len(cs.Hosts))
	for _, hostPort := range cs.Hosts {
		host, port, err := net.SplitHostPort(hostPort)
		if err != nil {
			// Handle cases like "localhost" where port is missing
			if addrErr, ok := err.(*net.AddrError); ok && strings.Contains(addrErr.Err, "missing port") {
				host = hostPort
			} else {
				return nil, nil, fmt.Errorf("failed to split host and port from '%s': %w", hostPort, err)
			}
		}
		hosts = append(hosts, host)
		ports = append(ports, port)
	}

	return hosts, ports, nil
}

func (c *AtlasClient) GetClusterLogsForHost(ctx context.Context, projectID, host string, startDate *int64, endDate *int64) (string, error) {
	params := &admin.GetHostLogsApiParams{
		GroupId:   projectID,
		HostName:  host,
		LogName:   "mongodb",
		StartDate: startDate,
		EndDate:   endDate,
	}
	log, response, err := c.AtlasSDK.MonitoringAndLogsApi.GetHostLogsWithParams(ctx, params).Execute()
	if err != nil {
		Logger.Error("Failed to get host logs: ", err)
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		Logger.Error("Host log request not OK")
		return "", fmt.Errorf("host logs returned a non-200 response: %d", response.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("mongod_%s_%d_%d_*.log.gz", host, startDate, endDate))
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()
	defer log.Close()

	_, err = io.Copy(tmpFile, log)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	return tmpFile.Name(), nil
}

func (c *AtlasClient) GetDiskMetrics(ctx context.Context, projectID, host, partition *string, startDate *time.Time, endDate *time.Time, period *string, granularity *string) (*admin.ApiMeasurementsGeneralViewAtlas, error) {
	params := &admin.GetDiskMeasurementsApiParams{
		GroupId:       *projectID,
		PartitionName: *partition,
		ProcessId:     *host,
		Start:         startDate,
		End:           endDate,
		Granularity:   granularity,
	}
	if period != nil {
		params.Period = period
	} else if startDate != nil && endDate != nil {
		params.Start = startDate
		params.End = endDate
	}
	measurements, response, err := c.AtlasSDK.MonitoringAndLogsApi.GetDiskMeasurementsWithParams(ctx, params).Execute()
	if err != nil {
		Logger.Error("Failed to get disk metrics: ", err)
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		Logger.Error("Disk metrics request for host not OK")
		return nil, fmt.Errorf("disk metrics for host returned a non-200 response: %d", response.StatusCode)
	}
	return measurements, nil
}
