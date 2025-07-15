package main

import (
	"context"

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
