package main

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

// convert2parquet converts vcf to dataframe parquet file with variantkey + numalts
func convert2parquet(vcfPath, outputFilename, outputPath string, useSampleAsColumn, outputAllVcfColumns bool) {

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

	schemaDef, err := defineSchema(outputFilename, outputAllVcfColumns, useSampleAsColumn)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	fw := goparquet.NewFileWriter(f,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	// Read VCF file into query stream
	vcfScanner, header, err := vcfio.ReadNewVcf(vcfPath)
	if err != nil {
		log.Fatalf("Error reading vcf, %v\n", err)
	}

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
		infoStr := fields[7]

		outputMap := formatOutputMap(outputFilename, useSampleAsColumn, outputAllVcfColumns, variant, quality, genotypes, infoStr)

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

func defineSchema(outputFilename string, outputAllVcfColumns, useSampleAsColumn bool) (*parquetschema.SchemaDefinition, error) {
	var msgStr string
	switch {
	case outputAllVcfColumns:
		msgStr = `message test {
			required binary variantkey (STRING);
			required binary chrom (STRING);
			required int32 pos;
			required binary ref (STRING);
			required binary alt (STRING);
			required double qual;
			required binary filter (STRING);
			required binary info (STRING);
			required int32 numalts;
		}`

	default:
		msgStr = `message test {
		required binary variantkey (STRING);
		required int32 numalts;
	}`
	}

	if useSampleAsColumn {
		msgStr = strings.Replace(msgStr, "numalts", outputFilename, -1)
	}

	return parquetschema.ParseSchemaDefinition(msgStr)
}

func formatOutputMap(outputFilename string, useSampleAsColumn, outputAllVcfColumns bool, v vcfio.VariantInfo, q vcfio.Quality, g []vcfio.SampleSpecific, info string) map[string]interface{} {
	numaltsColumnName := "numalts"
	if useSampleAsColumn {
		numaltsColumnName = outputFilename
	}

	var numalts int32
	if len(g) != 0 {
		numalts = int32(g[0].NumAlts)
	}

	switch {
	case outputAllVcfColumns:
		return map[string]interface{}{
			"variantkey":      []byte(v.VariantKey),
			"chrom":           []byte(v.Chr),
			"pos":             int32(v.Start + 1),
			"ref":             []byte(v.Ref),
			"alt":             []byte(v.Alt),
			"qual":            q.QualScore,
			"filter":          []byte(q.Filter),
			"info":            []byte(info),
			numaltsColumnName: numalts,
		}
	default:
		return map[string]interface{}{
			"variantkey":      []byte(v.VariantKey),
			numaltsColumnName: numalts,
		}
	}
}
