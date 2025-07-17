package main

import (
	"encoding/json"
	"fmt"
)

func GetSlowQueriesPrompt(sqs []SlowQueryEntry, sqh []SlowQueryByDriver) (string, error) {
	prompt := "# Slow query analysis: \n\n"
	prompt += "Your job is to generate a markdown report analyzing the provided MongoDB slow queries. Focus on why they are slow (e.g., missing indexes, query antipatterns, etc). Keep it concise, and as pragmatic as possible - use lists for your findings, and address the stats and details provided and how improving each query can benefit them (e.g., less bytes read means less disk pressure, etc.).\n"
	prompt += "For the ESR rule: Analyze the role of each field in the query (equality, sort, or range - remember that only direct equality and the $in operator are considered equality operators). \n"
	prompt += "Don't just point out whether an index is being used - suggest superior indexes when applicable.\n"
	prompt += "Mention the originating driver - it helps the report reader understand where a query is coming from.\n"
	prompt += "In addition, you can use the slowest query log provided with each query shape to convey your points.\n"
	prompt += "For each query shape section, add the sample slow query as a code block, so that the reader can identify the analyzed query.\n"
	prompt += "If you're going to suggest indexes, take MongoDB's ESR guideline for indexes into consideration.\n"
	prompt += fmt.Sprintf("there are %d slow query shapes to analyze. Please analyze them, each getting its own section in the markdown. Below are the slowest queries from each query shape:\n", len(sqs))
	for i, sq := range sqs {
		sqd := sqh[i]
		prompt += fmt.Sprintf("\n## Slow query shape no. %d\n\n", i+1)
		prompt += fmt.Sprintf("Query shape appearances: %d\n", sqd.Count)
		prompt += fmt.Sprintf("Avg Bytes Read: %f\n", sqd.AvgBytesRead)
		prompt += fmt.Sprintf("Avg Bytes Written: %f\n", sqd.AvgWritten)
		prompt += fmt.Sprintf("Avg Duration Millis: %f\n", sqd.AvgDurationMillis)
		prompt += fmt.Sprintf("Total Duration of slow queries (Millis): %d\n", sqd.TotalDurationMillis)
		prompt += fmt.Sprintf("Avg Num Yields: %f\n", sqd.AvgNumYields)
		prompt += fmt.Sprintf("Originating driver: %s\n", sq.Driver)
		prompt += "Slowest query log:\n\n"
		prompt += "```json\n"
		attr := sq.Attr
		js, err := json.MarshalIndent(attr, "", "  ")
		if err != nil {
			Logger.Error(err)
			return "", err
		}
		prompt += string(js)
		prompt += "\n```\n\n"
	}
	return prompt, nil
}

func GetMetricsAnalysisPrompt() (string, error) {
	return `Markdown response, and no intro text:
The attached files contain Normalized CPU information about a node in a MongoDB cluster. Each measurement. Please share your opinion about 
the measurements. Focus on normalized CPU, and share your observations about how busy the cluster is.
Desired sections: Disk, memory, and query targeting. 
QUERY_TARGETING_SCANNED_PER_RETURNED and QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED pertain to (scanned index keys/returned documents), and (scanned documents/returned documents), respectively;
SYSTEM_NORMALIZED_CPU_USER pertains to the CPU utilization.
SYSTEM_MEMORY_USED and SYSTEM_MEMORY_AVAILABLE pertain to RAM usage.
Keep you answers brief and concise, and share your opinion on each section.`, nil
}
