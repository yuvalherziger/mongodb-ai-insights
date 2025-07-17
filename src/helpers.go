package main

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

func parseISODuration(durationStr string) (time.Duration, error) {
	// A simple regex to extract components for common ISO8601 durations
	re := regexp.MustCompile(`P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?`)
	matches := re.FindStringSubmatch(durationStr)

	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid ISO8601 duration format: %s", durationStr)
	}

	var d time.Duration

	// Note: This example only handles H, M, S for simplicity.
	// For Y, Mon, D, you'd need to consider calendar arithmetic.
	if matches[4] != "" { // Hours
		hours, _ := strconv.Atoi(matches[4])
		d += time.Duration(hours) * time.Hour
	}
	if matches[5] != "" { // Minutes
		minutes, _ := strconv.Atoi(matches[5])
		d += time.Duration(minutes) * time.Minute
	}
	if matches[6] != "" { // Seconds
		seconds, _ := strconv.Atoi(matches[6])
		d += time.Duration(seconds) * time.Second
	}

	return d, nil
}

func ConvertISO8601DurationToUnixTimestamp(d string) (int64, int64) {
	duration, err := parseISODuration(d)
	if err != nil {
		panic(err)
	}
	now := time.Now()
	nowUnix := now.Unix()
	startTime := now.Add(-duration)
	startTimeUnix := startTime.Unix()
	return startTimeUnix, nowUnix
}
