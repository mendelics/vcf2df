package converter

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/mendelics/vcfio"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// Convert2parquet converts vcf to dataframe parquet file with variantkey + numalts
func Convert2parquet(vcfPath, outputFolder string) {

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
	schemaMessage, infoList, err := defineSchemaMessage(header)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	columnMap := mapColumnNames(infoList)

	schemaDef, err := parquetschema.ParseSchemaDefinition(schemaMessage)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	fw := goparquet.NewFileWriter(f,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
		goparquet.WithMetaData(columnMap),
	)

	for vcfScanner.Scan() {
		line := vcfScanner.Text()

		// Parse vcf line into variant struct
		variant, quality, genotypes := parseVariant(line, header)

		// Alts with * are skipped
		if variant.Alt == "*" {
			continue
		}

		fields := strings.Split(line, "\t")
		infos := vcfio.NewInfoByte([]byte(fields[7]), header)

		if len(genotypes) != 0 {
			for i := range genotypes {
				if genotypes[i].NumAlts != 0 {
					outputMap := formatOutputMap(variant, quality, genotypes[i], infoList, infos)

					if err := fw.AddData(outputMap); err != nil {
						log.Fatalf("Failed to add input %s to parquet file: %v", variant.VariantKey, err)
					}
				}
			}
		} else {
			outputMap := formatOutputMap(variant, quality, vcfio.SampleSpecific{}, infoList, infos)

			if err := fw.AddData(outputMap); err != nil {
				log.Fatalf("Failed to add input %s to parquet file: %v", variant.VariantKey, err)
			}
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet file writer failed: %v", err)
	}

	outputFile.Sync()
	outputFile.Close()

	log.Printf("Completed in %.2f seconds\n", time.Since(startTime).Seconds())
}
