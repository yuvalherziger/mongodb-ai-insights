package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mongodb-forks/digest"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

var (
	ONE_MINUTE = "PT1M"
	ONE_HOUR   = "PT1H"
	ONE_DAY    = "P1D"
)

type DataPoints struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value,omitempty"`
}

type Measurement struct {
	ClusterName string       `json:"clusterName"`
	Name        string       `json:"name"`
	Units       string       `json:"units"`
	DataPoints  []DataPoints `json:"dataPoints"`
}

type Link struct {
	Href string `json:"href"`
	Rel  string `json:"rel"`
}

type MeasurementsResponse struct {
	Start        string        `json:"start"`
	End          string        `json:"end"`
	Granularity  string        `json:"granularity"`
	GroupId      string        `json:"groupId"`
	HostId       string        `json:"hostId"`
	Links        []Link        `json:"links"`
	ProcessId    string        `json:"processId"`
	Measurements []Measurement `json:"measurements"`
}

type AtlasClusterInfo struct {
	ConnectionStrings struct {
		Standard    string `json:"standard"`
		StandardSrv string `json:"standardSrv"`
	} `json:"connectionStrings"`
}

type AtlasClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

type MeasurementsRequestSettings struct {
	ProjectId   string
	ProcessId   string
	PublicKey   string
	PrivateKey  string
	Start       string
	End         string
	Period      string
	Granularity string
	Metrics     *[]string
}

const atlasAPIBaseURL = "https://cloud.mongodb.com"

func NewAtlasClient(httpClient *http.Client) *AtlasClient {
	if httpClient == nil {
		// Always use a new http.Client to avoid sharing the default client's Transport (which may be nil)
		httpClient = &http.Client{}
	}
	return &AtlasClient{
		BaseURL:    atlasAPIBaseURL,
		HTTPClient: httpClient,
	}
}

func (c *AtlasClient) GetMeasurementsForHost(ctx context.Context, opts *MeasurementsRequestSettings) (*MeasurementsResponse, error) {
	u, _ := url.Parse(c.BaseURL)
	q := u.Query()
	if opts.Period != "" {
		q.Set("period", opts.Period)
	} else {
		if opts.Start == "" {
			oneDayAgo := time.Now().AddDate(0, 0, -1).UTC()
			opts.Start = oneDayAgo.Format(time.RFC3339)
		}
		if opts.Start == "" {
			currentTime := time.Now().UTC()
			opts.End = currentTime.Format(time.RFC3339)
		}
		q.Set("start", opts.Start)
		q.Set("end", opts.End)
	}
	if opts.Granularity == "" {
		opts.Granularity = ONE_MINUTE
	}

	u.Path = fmt.Sprintf("/api/atlas/v2/groups/%s/processes/%s/measurements",
		url.PathEscape(opts.ProjectId),
		url.PathEscape(opts.ProcessId),
	)
	q.Set("granularity", opts.Granularity)
	if opts.Metrics != nil && len(*opts.Metrics) > 0 {
		for _, m := range *opts.Metrics {
			q.Add("m", m)
		}
	}
	u.RawQuery = q.Encode()
	uri := u.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.atlas.2025-03-12+json")

	// Use Digest authentication
	t := &digest.Transport{
		Username: opts.PublicKey,
		Password: opts.PrivateKey,
	}
	client := c.HTTPClient
	if client == http.DefaultClient {
		client = &http.Client{
			Transport: t,
			Timeout:   c.HTTPClient.Timeout,
		}
	} else {
		baseTransport := c.HTTPClient.Transport
		if baseTransport == nil {
			baseTransport = http.DefaultTransport
		}
		t.Transport = baseTransport
		client = &http.Client{
			Transport: t,
			Timeout:   c.HTTPClient.Timeout,
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	var r MeasurementsResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return &r, nil
}

func (c *AtlasClient) getAtlasClusterInfo(ctx context.Context, publicKey, privateKey, projectID, clusterName string) (*AtlasClusterInfo, error) {
	url := fmt.Sprintf("%s/api/atlas/v2/groups/%s/clusters/%s", c.BaseURL, projectID, clusterName)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.atlas.2025-03-12+json")

	// Use Digest authentication
	digestTransport := &digest.Transport{
		Username: publicKey,
		Password: privateKey,
	}
	client := c.HTTPClient
	if client == http.DefaultClient {
		client = &http.Client{
			Transport: digestTransport,
			Timeout:   c.HTTPClient.Timeout,
		}
	} else {
		baseTransport := c.HTTPClient.Transport
		if baseTransport == nil {
			baseTransport = http.DefaultTransport
		}
		digestTransport.Transport = baseTransport
		client = &http.Client{
			Transport: digestTransport,
			Timeout:   c.HTTPClient.Timeout,
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var info AtlasClusterInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &info, nil
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

func (c *AtlasClient) DownloadClusterLogs(ctx context.Context, publicKey, privateKey, projectID, clusterName string, startDate int64, endDate int64) (map[string]string, error) {
	Logger.Info("Downloading Atlas cluster logs")
	var hostLogMapping = make(map[string]string)
	atlasClusterInfo, error := c.getAtlasClusterInfo(ctx, publicKey, privateKey, projectID, clusterName)
	if error != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", error)
	}
	hosts, ports, err := GetHostsFromConnectionString(atlasClusterInfo.ConnectionStrings.Standard)
	if err != nil {
		return nil, fmt.Errorf("failed to get hosts from connection string: %w", err)
	}
	var logFiles []string
	for i, host := range hosts {
		Logger.WithFields(logrus.Fields{"host": host}).Info("Downloading logs for host")
		logFile, err := c.downloadClusterLogsForHost(ctx, publicKey, privateKey, projectID, host, startDate, endDate)
		if err != nil {
			_ = c.DeleteClusterLogs(ctx, logFiles)
			return nil, fmt.Errorf("failed to download logs for host %s: %w", host, err)
		}
		hostLogMapping[fmt.Sprintf("%s:%s", host, ports[i])] = logFile
		logFiles = append(logFiles, logFile)
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

func (c *AtlasClient) downloadClusterLogsForHost(ctx context.Context, publicKey, privateKey, projectID, host string, startDate int64, endDate int64) (string, error) {
	url := fmt.Sprintf(
		"%s/api/atlas/v2/groups/%s/clusters/%s/logs/mongodb.gz?endDate=%d&startDate=%d",
		c.BaseURL, projectID, host, endDate, startDate,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.atlas.2023-02-01+gzip")
	req.Header.Set("Content-Type", "application/gzip")

	digestTransport := &digest.Transport{
		Username: publicKey,
		Password: privateKey,
	}
	client := c.HTTPClient
	if client == http.DefaultClient {
		client = &http.Client{
			Transport: digestTransport,
			Timeout:   c.HTTPClient.Timeout,
		}
	} else {
		baseTransport := c.HTTPClient.Transport
		if baseTransport == nil {
			baseTransport = http.DefaultTransport
		}
		digestTransport.Transport = baseTransport
		client = &http.Client{
			Transport: digestTransport,
			Timeout:   c.HTTPClient.Timeout,
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("mongod_%s_%d_%d_*.log.gz", host, startDate, endDate))
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write log to temp file: %w", err)
	}

	return tmpFile.Name(), nil
}
