package main

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/mendelics/vcfio"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

// convert2parquet converts vcf to dataframe parquet file with variantkey + numalts
func convert2parquet(vcfPath, outputFolder string) {

	// Check VCF file
	missingVcfs := checkIfVcfsExist([]string{vcfPath})
	if len(missingVcfs) != 0 {
		log.Fatalf("missing vcfs: %v", missingVcfs)
	}

	outputPath := createOutputFile(vcfPath, outputFolder)

	startTime := time.Now()
	log.Printf("Starting conversion to dataframe.")

	outputFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("Error creating output file, %v\n", err)
	}

	f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Opening output file failed: %v", err)
	}
	defer f.Close()

	// Read VCF file into query stream
	vcfScanner, header, err := vcfio.ReadNewVcf(vcfPath)
	if err != nil {
		log.Fatalf("Error reading vcf, %v\n", err)
	}

	// Define output schema
	schemaDef, infoList, err := defineSchema(header)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	fw := goparquet.NewFileWriter(f,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	for vcfScanner.Scan() {
		line := vcfScanner.Text()

		// Parse vcf line into variant struct
		variant, quality, genotypes := parseVariant(line, header)

		// Alts with * are skipped
		if variant.Alt == "*" {
			continue
		}

		if genotypes[0].NumAlts == 0 {
			continue
		}

		fields := strings.Split(line, "\t")
		infos := vcfio.NewInfoByte([]byte(fields[7]), header)

		outputMap := formatOutputMap(variant, quality, genotypes, infoList, infos)

		if err := fw.AddData(outputMap); err != nil {
			log.Fatalf("Failed to add input %s to parquet file: %v", variant.VariantKey, err)
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet file writer failed: %v", err)
	}

	outputFile.Sync()
	outputFile.Close()

	log.Printf("Completed in %.2f seconds\n", time.Since(startTime).Seconds())
}
