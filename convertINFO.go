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

// ConvertBETA converts vcf to dataframe parquet file with variantkey + beta
func ConvertBETA(
	vcf string,
	outputFolder string,
) {
	missingVcfs := checkIfVcfsExist([]string{vcf})
	if len(missingVcfs) != 0 {
		log.Fatalf("missing vcfs: %v", missingVcfs)
	}

	prsName := filepath.Base(vcf)
	prsName = strings.Replace(prsName, ".vcf.gz", "", -1)

	outputFileName := path.Join(outputFolder, fmt.Sprintf("%s.parquet", prsName))
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("Error creating output file, %v\n", err)
	}

	f, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Opening output file failed: %v", err)
	}
	defer f.Close()

	schemaStr := `message test {
		required binary variantkey (STRING);
		required double PRSNAME;
	}`

	schemaStr = strings.Replace(schemaStr, "PRSNAME", prsName, -1)
	schemaDef, err := parquetschema.ParseSchemaDefinition(schemaStr)
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

		var beta float64
		fields := strings.Split(line, "\t")
		info := vcfio.NewInfoByte([]byte(fields[7]), header)
		betaInterface, err := info.Get("BETA")
		if err == nil && betaInterface != nil {
			beta = betaInterface.(float64)
		}

		if err := fw.AddData(map[string]interface{}{
			"variantkey": []byte(variantInfo.VariantKey),
			prsName:      beta,
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
