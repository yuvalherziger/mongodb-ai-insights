package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/genai"
)

type LLMClient struct {
	GeminiClient *genai.Client
}

func NewLLMClient(geminiClient *genai.Client) *LLMClient {
	return &LLMClient{
		GeminiClient: geminiClient,
	}
}

const defaultModel = "gemini-2.5-pro"

func (c *LLMClient) GetMetricInsights(ctx context.Context, files []string, prompt string, modelName string) (*genai.GenerateContentResponse, error) {
	if modelName == "" {
		modelName = defaultModel
	}
	uris, err := c.uploadContextFiles(ctx, files)
	if err != nil {
		return nil, err
	}
	var parts []*genai.Part
	for _, file := range uris {
		parts = append(parts, genai.NewPartFromURI(file.URI, file.MIMEType))
	}

	parts = append(parts, genai.NewPartFromText("\n\n"))
	parts = append(parts, genai.NewPartFromText(prompt))
	contents := []*genai.Content{
		genai.NewContentFromParts(parts, "user"),
	}
	response, err := c.GeminiClient.Models.GenerateContent(ctx, modelName, contents, nil)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *LLMClient) uploadContextFiles(ctx context.Context, files []string) ([]genai.File, error) {
	var uris []genai.File

	for _, f := range files {
		file, err := c.GeminiClient.Files.UploadFromPath(
			ctx,
			f,
			&genai.UploadFileConfig{
				MIMEType: "text/plain",
			},
		)
		if err != nil {
			return nil, err
		}
		uris = append(uris, *file)

	}
	return uris, nil
}

func (c *LLMClient) GenerateSlowQueryReport(ctx context.Context, dbName string) error {
	cfg, _ := GetConfig()
	modelName := cfg.GeminiModel
	if modelName == "" {
		modelName = defaultModel
	}
	slowestQueryHashes, err := GetTopQueryShapesByExecutionTime(ctx, dbName, cfg.NumAnalyzedQueries)
	if err != nil {
		Logger.Error(err)
		return err
	}
	var slowestQueries []SlowQueryEntry
	for _, qHash := range slowestQueryHashes {
		//var id bson.M
		id := qHash.ID
		driver := id.Driver
		queryHash := id.Hash
		if queryHash == "" {
			continue
		}
		sq, err := GetSlowestQueryByShape(ctx, dbName, queryHash, driver)
		if err != nil {
			Logger.Error(err)
			return err
		}
		slowestQueries = append(slowestQueries, sq)
	}
	prompt, err := GetSlowQueriesPrompt(slowestQueries, slowestQueryHashes)
	if err != nil {
		Logger.Error(err)
		return err
	}
	var parts []*genai.Part
	parts = append(parts, genai.NewPartFromText("\n\n"))
	parts = append(parts, genai.NewPartFromText(prompt))
	contents := []*genai.Content{
		genai.NewContentFromParts(parts, "user"),
	}
	//_, err = c.GeminiClient.Models.GenerateContent(ctx, modelName, contents, nil)
	response, err := c.GeminiClient.Models.GenerateContent(ctx, modelName, contents, nil)
	if err != nil {
		Logger.Error(err)
		return err
	}
	resFile, err := os.Create(cfg.SlowQueriesReportOutputFile)
	if err != nil {
		Logger.Fatalf("Failed to create result file: %v", err)
	}
	defer resFile.Close()
	if _, err := resFile.Write([]byte(response.Text())); err != nil {
		//if _, err := resFile.Write([]byte(prompt)); err != nil {
		Logger.Fatalf("Failed to write results: %v", err)
	}
	Logger.WithFields(logrus.Fields{"outputFile": resFile.Name()}).Info("Slow query report written to the filesystem")
	return nil
}

func (c *LLMClient) GenerateMetricsAnalysisReport(ctx context.Context, ac *AtlasClient, dbName string) error {
	cfg, err := GetConfig()
	hostnames, err := GetHostNames(ctx, dbName)
	if err != nil {
		panic(err)
	}
	var metricFiles []string

	var eventStrings []string
	// Iterate hostLogMapping keys and values, and use GetPrimaryElectionEvents
	for _, host := range hostnames {
		events, err := GetPrimaryElectionEvents(ctx, dbName)
		if err != nil {
			panic(err)
		}
		for _, eventTime := range events {
			eventStrings = append(eventStrings, fmt.Sprintf("%s became primary on %s", host, eventTime))
		}

		res, err := ac.GetMeasurementsForProcess(ctx, cfg.ProjectId, host, nil, nil, &cfg.Period, &cfg.MetricsGranularity)
		if err != nil {
			panic(err)
		}
		partitions, err := ac.GetDisksOnHost(ctx, cfg.ProjectId, host)
		if err != nil {
			panic(err)
		}

		for _, p := range *partitions {
			partition := p.PartitionName
			res, err := ac.GetDiskMetrics(ctx, &cfg.ProjectId, &host, partition, nil, nil, &cfg.Period, &cfg.MetricsGranularity)
			if err != nil {
				Logger.Fatalf("Failed to get measurements: %v", err)
			}
			jsonData, err := json.Marshal(res)
			if err != nil {
				Logger.Fatalf("Failed to marshal result to JSON: %v", err)
			}
			tmpFile, err := os.CreateTemp("", "disk-measurements-*.json")
			if err != nil {
				Logger.Fatalf("Failed to create temporary file: %v", err)
			}
			defer tmpFile.Close()
			if _, err := tmpFile.Write(jsonData); err != nil {
				Logger.Fatalf("Failed to write JSON to temporary file: %v", err)
			}
			Logger.WithFields(logrus.Fields{"contextFilePath": tmpFile.Name()}).Info("Disk metrics JSON file written")
			metricFiles = append(metricFiles, tmpFile.Name())
		}

		if err != nil {
			Logger.Fatalf("Failed to get measurements: %v", err)
		}
		jsonData, err := json.Marshal(res)
		if err != nil {
			Logger.Fatalf("Failed to marshal result to JSON: %v", err)
		}
		tmpFile, err := os.CreateTemp("", "measurements-*.json")
		if err != nil {
			Logger.Fatalf("Failed to create temporary file: %v", err)
		}
		defer tmpFile.Close()
		// Write the JSON data to the temporary file.
		if _, err := tmpFile.Write(jsonData); err != nil {
			Logger.Fatalf("Failed to write JSON to temporary file: %v", err)
		}
		Logger.WithFields(logrus.Fields{"contextFilePath": tmpFile.Name()}).Info("Host metrics JSON file written")
		metricFiles = append(metricFiles, tmpFile.Name())
	}

	prompt, _ := GetMetricsAnalysisPrompt()
	diskInfo, err := ac.GetAtlasClusterInfoString(ctx, cfg.ProjectId, cfg.ClusterName)
	if err != nil {
		panic(err)
	}
	finalPrompt := fmt.Sprintf(
		"%s. Important additional context on when nodes became primary in the cluster: %s. %s. Take into account this information when analyzing the data.",
		prompt,
		strings.Join(eventStrings, ". "),
		diskInfo,
	)
	insights, err := c.GetMetricInsights(
		context.Background(),
		metricFiles,
		finalPrompt,
		cfg.GeminiModel,
	)
	if err != nil {
		panic(err)
	}

	resFile, err := os.Create(cfg.MetricsReportOutputFile)
	if err != nil {
		Logger.Fatalf("Failed to create result file: %v", err)
	}
	defer resFile.Close()
	if _, err := resFile.Write([]byte(insights.Text())); err != nil {
		Logger.Fatalf("Failed to write results: %v", err)
	}

	Logger.WithFields(logrus.Fields{"outputFile": resFile.Name()}).Info("Results written to the filesystem")
	return nil
}
