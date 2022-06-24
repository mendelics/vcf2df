package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/mendelics/vcfio"
)

func createOutputFile(vcfPath, outputFolder string, useSampleAsFilename bool) (string, string) {
	switch {
	case useSampleAsFilename:
		// Read header
		vcfReader, err := vcfio.ReadVcf(vcfPath)
		if err != nil {
			log.Fatalf("error reading vcf: %v", err)
		}

		outputFilename := vcfReader.Header.SampleNames[0]
		outputPath := path.Join(outputFolder, fmt.Sprintf("%s.parquet", outputFilename))

		return outputFilename, outputPath

	default:
		outputFilename := filepath.Base(vcfPath)
		outputFilename = strings.Replace(outputFilename, ".vcf.gz", "", -1)
		outputFilename = strings.Replace(outputFilename, ".", "_", -1)

		outputPath := path.Join(outputFolder, fmt.Sprintf("%s.parquet", outputFilename))

		return outputFilename, outputPath
	}
}

func checkIfVcfsExist(vcfFiles []string) []string {
	missingVcfs := make([]string, 0)
	for _, vcf := range vcfFiles {
		if !fileExists(vcf) {
			missingVcfs = append(missingVcfs, vcf)
		}
	}
	return missingVcfs
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
