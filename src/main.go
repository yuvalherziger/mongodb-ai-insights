package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/genai"
)

func main() {
	cfg, err := GetConfig()
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	ac := NewAtlasClient(nil)
	dbName := fmt.Sprintf("%s_%s_logs", cfg.ClusterName, time.Now().Format(time.RFC3339))
	dbName = strings.ReplaceAll(dbName, "-", "")
	dbName = strings.ReplaceAll(dbName, ":", "")
	dbName = strings.ReplaceAll(dbName, "+", "")
	err = InitDb(ctx, ac, dbName)
	if err != nil {
		Logger.Error(err)
		panic(err)
	}
	geminiClient, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  cfg.GeminiAPIKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		Logger.Error(err)
		os.Exit(1)
	}
	lc := NewLLMClient(geminiClient)
	err = lc.GenerateSlowQueryReport(ctx, dbName)
	if err != nil {
		Logger.Error("Failed to generate slow query report", err)
		os.Exit(1)
	}
	err = lc.GenerateMetricsAnalysisReport(ctx, ac, dbName)
	if err != nil {
		Logger.Error("Failed to generate metrics analysis report", err)
		os.Exit(1)
	}
}
