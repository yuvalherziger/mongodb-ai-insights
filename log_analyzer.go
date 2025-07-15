package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const transitionToPrimary = "Transition to primary complete"

type LogEntry struct {
	Time struct {
		Date string `json:"$date"`
	} `json:"t"`
	Severity  string                 `json:"s"`
	Component string                 `json:"c"`
	ID        int64                  `json:"id"`
	Context   string                 `json:"ctx"`
	Message   string                 `json:"msg"`
	Attr      map[string]interface{} `json:"attr"`
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
			primaryTransitionTimes = append(primaryTransitionTimes, entry.Time.Date)
		}
	}
	return primaryTransitionTimes, nil
}
