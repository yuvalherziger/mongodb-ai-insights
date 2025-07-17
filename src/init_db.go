package main

import "context"

func InitDb(ctx context.Context, ac *AtlasClient, dbName string) error {
	cfg, err := GetConfig()
	start, end := ConvertISO8601DurationToUnixTimestamp(cfg.Period)
	hostLogMapping, err := ac.DownloadClusterLogs(ctx, cfg.AtlasPublicKey, cfg.AtlasPrivateKey, cfg.ProjectId, cfg.ClusterName, start, end)
	if err != nil {
		return err
	}
	fileReader := &DefaultFileReader{}
	for host, logFile := range hostLogMapping {
		AnalyzeLogStream(ctx, fileReader, logFile, host, dbName)
	}
	err = CreateSlowQueriesByDriver(ctx, dbName)
	if err != nil {
		Logger.Error("Error grouping slow queries by driver", err)
		return err
	}
	err = CreateIndexes(ctx, dbName)
	if err != nil {
		Logger.Error("Error creating indexes", err)
		return err
	}
	return nil
}
