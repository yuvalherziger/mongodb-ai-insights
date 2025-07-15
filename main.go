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

const prompt = `Markdown response,, and no intro text; just markdown:
the attached files contain Normalized CPU information about a node in a MongoDB cluster. Each measurement. Please share your opinion about 
the measurements. Focus on normalized CPU, and share your observations about how busy the cluster is.
Desired sections: Disk, memory, and query targeting. 
QUERY_TARGETING_SCANNED_PER_RETURNED and QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED pertain to (scanned index keys/returned documents), and (scanned documents/returned documents), respectively;
SYSTEM_NORMALIZED_CPU_USER pertains to the CPU utilization.
SYSTEM_MEMORY_USED and SYSTEM_MEMORY_AVAILABLE pertain to RAM usage.
Keep you answers brief and concise, and share your opinion on each section.`

func main() {
	cfg, err := GetConfig()
	Logger.WithFields(logrus.Fields{
		"projectId": cfg.ProjectId,
	}).Info("Config loaded")
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	c := NewAtlasClient(nil)
	start, end := ConvertISO8601DurationToUnixTimestamp(cfg.Period)
	hostLogMapping, err := c.DownloadClusterLogs(ctx, cfg.AtlasPublicKey, cfg.AtlasPrivateKey, cfg.ProjectId, cfg.ClusterName, start, end)
	if err != nil {
		panic(err)
	}
	fileReader := &DefaultFileReader{}
	var metricFiles []string

	var eventStrings []string
	// Iterate hostLogMapping keys and values, and use GetPrimaryElectionEvents
	for host, logFile := range hostLogMapping {
		events, err := GetPrimaryElectionEvents(fileReader, logFile)
		if err != nil {
			panic(err)
		}
		for _, eventTime := range events {
			eventStrings = append(eventStrings, fmt.Sprintf("%s became primary on %s", host, eventTime))
		}
		reqCfg := MeasurementsRequestSettings{
			Period:     cfg.Period,
			ProjectId:  cfg.ProjectId,
			ProcessId:  host,
			PublicKey:  cfg.AtlasPublicKey,
			PrivateKey: cfg.AtlasPrivateKey,
			Metrics:    &cfg.Metrics,
		}
		res, err := c.GetMeasurementsForHost(ctx, &reqCfg)
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
		Logger.WithFields(logrus.Fields{"contextFilePath": tmpFile.Name()}).Info("Context JSON file written")
		metricFiles = append(metricFiles, tmpFile.Name())
	}

	geminiClient, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  cfg.GeminiAPIKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		panic(err)
	}
	llmClient := NewLLMClient(geminiClient)
	finalPrompt := fmt.Sprintf(
		"%s. Important additional context on when nodes became primary in the cluster: %s. Take into account this information when analyzing the data.",
		prompt,
		strings.Join(eventStrings, ". "),
	)
	insights, err := llmClient.GetMetricInsights(
		context.Background(),
		metricFiles,
		finalPrompt,
		cfg.GeminiModel,
	)
	if err != nil {
		panic(err)
	}

	resFile, err := os.Create(cfg.OutputFile)
	if err != nil {
		Logger.Fatalf("Failed to create result file: %v", err)
	}
	defer resFile.Close()
	if _, err := resFile.Write([]byte(insights.Text())); err != nil {
		Logger.Fatalf("Failed to write results: %v", err)
	}

	Logger.WithFields(logrus.Fields{"outputFile": resFile.Name()}).Info("Results written to the filesystem")
}
