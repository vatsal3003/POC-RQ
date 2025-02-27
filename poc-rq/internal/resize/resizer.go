package resize

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/disintegration/imaging"
	"github.com/vatsal3003/poc-rq/pkg/models"
)

type ImageProcessor struct {
	configs []models.ResizeConfig
}

func NewImageProcessor(configs []models.ResizeConfig) *ImageProcessor {
	return &ImageProcessor{
		configs: configs,
	}
}

func (p *ImageProcessor) ProcessJob(job models.ResizeJob) error {
	// Open the original images
	src, err := imaging.Open(job.OriginalPath)
	if err != nil {
		return fmt.Errorf("failed to open image: %w", err)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(job.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get original file info
	fileExt := filepath.Ext(job.OriginalPath)
	fileName := filepath.Base(job.OriginalPath)
	fileNameWithoutExt := strings.TrimSuffix(fileName, fileExt)

	// Resize to all configured dimensions
	for _, config := range p.configs {
		dstImage := imaging.Resize(src, config.Width, config.Height, imaging.Lanczos)

		// Create the output file path
		outputPath := filepath.Join(job.OutputDir, fmt.Sprintf("%s_%s%s", fileNameWithoutExt, config.Suffix, fileExt))

		// Save the resized image
		err = imaging.Save(dstImage, outputPath)
		if err != nil {
			return fmt.Errorf("failed to save resized image %s: %w", outputPath, err)
		}
	}

	return nil
}
