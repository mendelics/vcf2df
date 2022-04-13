package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/mendelics/vcfio"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// ConvertNumAlts converts vcf to dataframe parquet file with variantkey + numalts
func ConvertNumAlts(
	vcf string,
	outputFolder string,
) {
	missingVcfs := checkIfVcfsExist([]string{vcf})
	if len(missingVcfs) != 0 {
		log.Fatalf("missing vcfs: %v", missingVcfs)
	}

	// Read VCF file into query stream
	vcfReader, err := vcfio.ReadVcf(vcf)
	if err != nil {
		log.Fatalf("error reading vcf: %v", missingVcfs)
	}

	outputFileName := path.Join(outputFolder, fmt.Sprintf("%s.parquet", vcfReader.Header.SampleNames[0]))
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("Error creating output file, %v\n", err)
	}

	f, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Opening output file failed: %v", err)
	}
	defer f.Close()

	schemaDef, err := parquetschema.ParseSchemaDefinition(
		`message test {
			required binary variantkey (STRING);
			required int32 numalts;
		}`)

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
		variantInfo, _, genotypes, _ := vcfio.ParseVariant(line, header)

		// Alts with * are skipped
		if variantInfo.Alt == "*" {
			continue
		}

		if genotypes[0].NumAlts == 0 {
			continue
		}

		if err := fw.AddData(map[string]interface{}{
			"variantkey": []byte(variantInfo.VariantKey),
			"numalts":    int32(genotypes[0].NumAlts),
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
