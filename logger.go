package main

import (
	"time" // Import the time package

	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

var Logger = func() *logrus.Logger {
	if logger != nil {
		return logger
	}
	l := logrus.New()

	// Set the formatter to include timestamps
	l.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

	cfg, _ := GetConfig()

	levelStr := cfg.LogLevel
	if levelStr == "" {
		levelStr = "info"
	}
	level, err := logrus.ParseLevel(levelStr)
	if err != nil {
		l.Warnf("Invalid LOG_LEVEL '%s', defaulting to info", levelStr)
		level = logrus.InfoLevel
	}
	l.SetLevel(level)
	logger = l
	return l
}()
