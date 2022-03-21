package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mendelics/vcf2df/vcfio"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// ConvertINFO converts vcf to dataframe parquet file with variantkey + info
func ConvertINFO(
	vcf string,
	outputFolder string,
	info string,
	infoType string,
) {
	missingVcfs := checkIfVcfsExist([]string{vcf})
	if len(missingVcfs) != 0 {
		log.Fatalf("missing vcfs: %v", missingVcfs)
	}

	outputFileName := filepath.Base(vcf)
	outputFileName = strings.Replace(outputFileName, ".vcf.gz", ".parquet", -1)
	outputFilePath := path.Join(outputFolder, outputFileName)

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Error creating output file, %v\n", err)
	}

	f, err := os.OpenFile(outputFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Opening output file failed: %v", err)
	}
	defer f.Close()

	var defType, defType2 string
	switch infoType {
	case "string":
		defType = "binary"
		defType2 = " (STRING)"
	case "int":
		defType = "int64"
	case "float":
		defType = "double"
	}

	definitionStr := fmt.Sprintf("`message test {\n\t\trequired binary variantkey (STRING);\n\t\trequired %s %s%s;\n\t}`", defType, info, defType2)

	schemaDef, err := parquetschema.ParseSchemaDefinition(definitionStr)

	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	fw := goparquet.NewFileWriter(f,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	startTime := time.Now()
	log.Printf("Starting conversion to dataframe.")

	var counter int
	annotTime := time.Now()

	// Read VCF file into query stream
	vcfScanner, header, err := vcfio.ReadNewVcf(vcf)
	if err != nil {
		log.Fatalf("Error reading vcf, %v\n", err)
	}

	for vcfScanner.Scan() {
		line := vcfScanner.Text()

		// Parse vcf line into variant struct
		variantInfo, _, _, _, _ := vcfio.ParseVariant(line, header)

		// Alts with * are skipped
		if variantInfo.Alt == "*" {
			continue
		}

		if err := fw.AddData(map[string]interface{}{
			"variantkey": []byte(variantInfo.VariantKey),
			info:         int32(genotypes[0].NumAlts),
		}); err != nil {
			log.Fatalf("Failed to add input %s to parquet file: %v", variantInfo.VariantKey, err)
		}

		counter++
		if counter%100000 == 0 {
			speed := float64(counter) / time.Since(annotTime).Seconds()
			log.Printf("Variants intersected: %d\tAnnotatation speed: %.0f variants/second\n", counter, speed)
		}
	}

	speed := float64(counter) / time.Since(annotTime).Seconds()
	log.Printf("Variants converted: %d\tAnnotatation speed: %.0f variants/second\n", counter, speed)

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet file writer failed: %v", err)
	}

	outputFile.Sync()
	outputFile.Close()

	log.Printf("Completed in %.1f seconds\n", time.Since(startTime).Seconds())
}
