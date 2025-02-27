package models

import (
	"github.com/google/uuid"
)

type ResizeJob struct {
	OriginalPath string `json:"original_path"`
	OutputDir    string `json:"output_dir"`
	JobID        string `json:"job_id"`
}

type ResizeConfig struct {
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Suffix string `json:"suffix"`
}

func NewResizeJob(originalPath, outputDir string) ResizeJob {
	return ResizeJob{
		OriginalPath: originalPath,
		OutputDir:    outputDir,
		JobID:        uuid.New().String(),
	}
}
