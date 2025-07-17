package main

import (
	"encoding/json"
	"os"
	"sync"
)

type Config struct {
	GeminiAPIKey                string   `json:"GeminiAPIKey"`
	AtlasPublicKey              string   `json:"atlasPublicKey"`
	AtlasPrivateKey             string   `json:"atlasPrivateKey"`
	Metrics                     []string `json:"metrics"`
	MetricsReportOutputFile     string   `json:"metricsReportOutputFile"`
	SlowQueriesReportOutputFile string   `json:"slowQueriesReportOutputFile"`
	GeminiModel                 string   `json:"geminiModel"`
	ProjectId                   string   `json:"projectId"`
	ClusterName                 string   `json:"clusterName"`
	Period                      string   `json:"period"`
	LogLevel                    string   `json:"logLevel"`
	OutputMongoURI              string   `json:"outputMongoUri"`
	NumAnalyzedQueries          int      `json:"numAnalyzedQueries"`
}

var (
	cfg           *Config
	once          sync.Once
	loadConfigErr error
)

func GetConfig() (*Config, error) {
	once.Do(func() {
		configPath := os.Getenv("REPORT_INSIGHTS_CONFIG_FILE")
		if configPath == "" {
			configPath = "./config.json"
		}

		var fileContents []byte
		fileContents, loadConfigErr = os.ReadFile(configPath)
		if loadConfigErr != nil {
			return
		}

		var tempCfg Config
		loadConfigErr = json.Unmarshal(fileContents, &tempCfg)
		if loadConfigErr != nil {
			return
		}

		cfg = &tempCfg
	})

	return cfg, loadConfigErr
}
