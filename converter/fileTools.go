package converter

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func createOutputFile(vcfPath, outputFolder string) (string, string) {
	outputFilename := filepath.Base(vcfPath)
	outputFilename = strings.Replace(outputFilename, ".vcf.gz", "", -1)
	outputFilename = strings.Replace(outputFilename, ".", "_", -1)

	outputPath := path.Join(outputFolder, fmt.Sprintf("%s.parquet", outputFilename))

	return outputPath, outputFilename
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
