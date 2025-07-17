package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const transitionToPrimary = "Transition to primary complete"
const slowQuery = "Slow query"
const clientMetadata = "\"msg\":\"client metadata\""
const batchSize = 5e3

type LogEntry struct {
	T struct {
		Date bson.DateTime `json:"$date"`
	} `json:"t"`
	S       string                 `json:"s"`
	C       string                 `json:"c"`
	ID      int64                  `json:"id"`
	Ctx     string                 `json:"ctx"`
	Msg     string                 `json:"msg"`
	Attr    map[string]interface{} `json:"attr"`
	Host    string                 `json:"host"`
	CtxHost string                 `json:"ctxHost"`
}

func (t *LogEntry) UnmarshalJSON(data []byte) error {
	type Alias LogEntry
	aux := &struct {
		Time struct {
			Date string `json:"$date"`
		} `json:"t"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339Nano, aux.Time.Date)
	if err != nil {
		return err
	}
	t.T.Date = bson.DateTime(parsed.UnixMilli())
	return nil
}

type FileReader interface {
	Open(filePath string) (io.ReadCloser, error)
	GetExtension(filePath string) string
}

type DefaultFileReader struct{}

func (d *DefaultFileReader) Open(filePath string) (io.ReadCloser, error) {
	return os.Open(filePath)
}

func (d *DefaultFileReader) GetExtension(filePath string) string {
	return strings.ToLower(filepath.Ext(filePath))
}

func GetPrimaryElectionEvents(fileReader FileReader, logPath string) ([]string, error) {
	file, err := fileReader.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	ext := fileReader.GetExtension(logPath)
	if ext == ".gz" {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer gzReader.Close()
		return analyzeLogStream(gzReader)
	}
	return analyzeLogStream(file)
}

func analyzeLogStream(r io.Reader) ([]string, error) {
	var primaryTransitionTimes []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, transitionToPrimary) {
			var entry LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				return nil, fmt.Errorf("failed to unmarshal response: %w", err)
			}
			primaryTransitionTimes = append(primaryTransitionTimes, time.UnixMilli(int64(entry.T.Date)).Format(time.RFC3339Nano))
		}
	}
	return primaryTransitionTimes, nil
}
func AnalyzeLogStream(ctx context.Context, fr FileReader, logPath string, host string, dbName string) error {
	Logger.Info("Analyzing log stream")
	file, err := fr.Open(logPath)
	var r io.Reader
	if err != nil {
		return err
	}
	defer file.Close()
	ext := fr.GetExtension(logPath)
	if ext == ".gz" {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gzReader.Close()
		r = gzReader
	} else {
		r = file
	}

	var primaryTransitionEntries []interface{}
	var slowQueryEntries []interface{}
	var clientMetadataEntries []interface{}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, transitionToPrimary) {
			var entry LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				return fmt.Errorf("failed to unmarshal response: %w", err)
			}
			entry.Host = host
			entry.CtxHost = fmt.Sprintf("%s_%s", entry.Ctx, entry.Host)
			primaryTransitionEntries = append(primaryTransitionEntries, entry)
		} else if strings.Contains(line, slowQuery) {
			var entry LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				return fmt.Errorf("failed to unmarshal response: %w", err)
			}
			entry.Host = host
			entry.CtxHost = fmt.Sprintf("%s_%s", entry.Ctx, entry.Host)
			slowQueryEntries = append(slowQueryEntries, entry)
		} else if strings.Contains(line, clientMetadata) {
			var entry LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				return fmt.Errorf("failed to unmarshal response: %w", err)
			}
			entry.Host = host
			entry.CtxHost = fmt.Sprintf("%s_%s", entry.Ctx, entry.Host)
			clientMetadataEntries = append(clientMetadataEntries, entry)
		}

		if len(primaryTransitionEntries) >= batchSize {
			Logger.WithFields(logrus.Fields{"batchSize": len(primaryTransitionEntries)}).Info("Writing primary transition batch")
			_, err := InsertPrimaryChangeEventBatch(ctx, primaryTransitionEntries, dbName)
			if err != nil {
				Logger.Error(err)
			}
			primaryTransitionEntries = nil
		}

		if len(slowQueryEntries) >= batchSize {
			Logger.WithFields(logrus.Fields{"batchSize": len(slowQueryEntries)}).Info("Writing slow queries batch")
			_, err := InsertSlowQueriesBatch(ctx, slowQueryEntries, dbName)
			if err != nil {
				Logger.Error(err)
			}
			slowQueryEntries = nil
		}

		if len(clientMetadataEntries) >= batchSize {
			Logger.WithFields(logrus.Fields{"batchSize": len(clientMetadataEntries)}).Info("Writing client metadata batch")
			_, err := InsertClientMetadataBatch(ctx, clientMetadataEntries, dbName)
			if err != nil {
				Logger.Error(err)
			}
			clientMetadataEntries = nil
		}
	}

	// Empty the remaining batches if there are any:
	if len(primaryTransitionEntries) > 0 {
		Logger.WithFields(logrus.Fields{"batchSize": len(primaryTransitionEntries)}).Info("Writing primary transition batch")
		_, err := InsertPrimaryChangeEventBatch(ctx, primaryTransitionEntries, dbName)
		if err != nil {
			Logger.Error(err)
		}
		primaryTransitionEntries = nil
	}

	if len(slowQueryEntries) > 0 {
		Logger.WithFields(logrus.Fields{"batchSize": len(slowQueryEntries)}).Info("Writing slow queries batch")
		_, err := InsertSlowQueriesBatch(ctx, slowQueryEntries, dbName)
		if err != nil {
			Logger.Error(err)
		}
		slowQueryEntries = nil
	}

	if len(clientMetadataEntries) > 0 {
		Logger.WithFields(logrus.Fields{"batchSize": len(clientMetadataEntries)}).Info("Writing client metadata batch")
		_, err := InsertClientMetadataBatch(ctx, clientMetadataEntries, dbName)
		if err != nil {
			Logger.Error(err)
		}
		clientMetadataEntries = nil
	}
	return nil
}
