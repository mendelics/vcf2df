package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/mendelics/vcfio"
)

type infoField struct {
	id       string
	infoType string
}

func defineSchema(header *vcfio.Header, betaOnly, numaltsOnly bool, outputFilename string) (*parquetschema.SchemaDefinition, []infoField, error) {
	var vcfColumns string
	samplesSchemaLines := make([]string, 0)
	infos := make([]string, 0)
	infoList := make([]infoField, 0)

	// Sample Genotypes
	switch {
	case betaOnly:
		// Do nothing

	case numaltsOnly:
		vcfColumns = "required int32 numalts;"

	default:
		vcfColumns = `
		required binary CHROM (STRING);
		required int32 POS;
		required binary REF (STRING);
		required binary ALT (STRING);
		required double QUAL;
		required binary FILTER (STRING);
`

		for _, sample := range header.SampleNames {
			sampleLine := fmt.Sprintf("required int32 NUMALTS@%s;\n", sample)
			samplesSchemaLines = append(samplesSchemaLines, sampleLine)
		}

	}

	// INFO
	switch {
	case numaltsOnly:
		// Do nothing

	default:
		for _, info := range header.Infos {
			if betaOnly && info.Id != "BETA" {
				continue
			}

			var line string
			var infoTypeStr string

			switch {
			case info.Type == "Integer" && info.Number == "1":
				line = fmt.Sprintf("required int32 %s;\n", info.Id)
				infoTypeStr = "int32"

			case info.Type == "Float" && info.Number == "1":
				line = fmt.Sprintf("required double %s;\n", info.Id)
				infoTypeStr = "float64"

			case info.Type == "Flag" && info.Number == "0":
				line = fmt.Sprintf("required boolean %s;\n", info.Id)
				infoTypeStr = "bool"

			case info.Type == "String" && info.Number == "1":
				line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
				infoTypeStr = "string"

			case info.Number != "1":

				switch {
				case info.Type == "Integer":
					line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
					infoTypeStr = "[]int"

				case info.Type == "Float":
					line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
					infoTypeStr = "[]float32"
				}

			default:
				continue
			}

			if betaOnly {
				line = strings.Replace(line, "BETA", outputFilename, -1)
			}

			infoList = append(infoList, infoField{
				id:       info.Id,
				infoType: infoTypeStr,
			})

			infos = append(infos, line)
		}
	}

	samplesStr := strings.Join(samplesSchemaLines, "")
	infoStr := strings.Join(infos, "")
	msg := fmt.Sprintf("message test {required binary variantkey (STRING);%s%s%s}", vcfColumns, samplesStr, infoStr)

	schemadef, err := parquetschema.ParseSchemaDefinition(msg)
	if err != nil {
		return nil, nil, err
	}

	return schemadef, infoList, nil
}

func formatOutputMap(
	v vcfio.VariantInfo,
	q vcfio.Quality,
	g []vcfio.SampleSpecific,
	infoList []infoField,
	infos *vcfio.InfoByte,
	betaOnly bool,
	numaltsOnly bool,
	outputFilename string,
) map[string]interface{} {

	// Every df contains variantkey
	outputFields := map[string]interface{}{
		"variantkey": []byte(v.VariantKey),
	}

	switch {
	case betaOnly:
		// Do nothing

	case numaltsOnly:
		if len(g) != 0 {
			outputFields["numalts"] = int32(g[0].NumAlts)
		} else {
			log.Fatalf("To output --numalts, must have at least 1 sample")
		}

	default:
		outputFields["CHROM"] = []byte(v.Chr)
		outputFields["POS"] = int32(v.Start + 1)
		outputFields["REF"] = []byte(v.Ref)
		outputFields["ALT"] = []byte(v.Alt)
		outputFields["QUAL"] = q.QualScore
		outputFields["FILTER"] = []byte(q.Filter)

		for _, sample := range g {
			columnName := fmt.Sprintf("NUMALTS@%s", sample.SampleName)
			outputFields[columnName] = int32(sample.NumAlts)
		}
	}

	switch {
	case numaltsOnly:
		// Do nothing

	default:
		// outputAllVcfColumns iterating over INFO fields
		for _, info := range infoList {
			if betaOnly && info.id != "BETA" {
				continue
			}

			valueInterface, err := infos.Get(info.id)

			if err == nil && valueInterface != nil {
				if betaOnly && info.id == "BETA" {
					outputFields[outputFilename] = valueInterface.(float64)
					continue
				}

				switch info.infoType {
				case "int32":
					value := valueInterface.(int)
					outputFields[info.id] = int32(value)

				case "float64":
					value := valueInterface.(float64)
					outputFields[info.id] = value

				case "float32":
					value := valueInterface.(float32)
					outputFields[info.id] = value

				case "bool":
					outputFields[info.id] = true

				case "[]int":
					value := valueInterface.([]int)
					valuesText := make([]string, 0)
					for i := range value {
						number := value[i]
						text := strconv.Itoa(number)
						valuesText = append(valuesText, text)
					}
					valueStr := strings.Join(valuesText, ",")
					outputFields[info.id] = []byte(valueStr)

				case "[]float32":
					value := valueInterface.([]float32)
					valuesText := make([]string, 0)
					for i := range value {
						fl := value[i]
						text := fmt.Sprintf("%.2f", fl)
						valuesText = append(valuesText, text)
					}
					valueStr := strings.Join(valuesText, ",")
					outputFields[info.id] = []byte(valueStr)

				case "string":
					value := valueInterface.(string)
					outputFields[info.id] = []byte(value)

				default:
					continue
				}
			} else {

				switch info.infoType {
				case "int32":
					outputFields[info.id] = int32(0)

				case "float64":
					outputFields[info.id] = float64(0.0)

				case "float32":
					outputFields[info.id] = float32(0.0)

				case "[]int":
					outputFields[info.id] = []byte("")

				case "[]float64":
					outputFields[info.id] = []byte("")

				case "bool":
					outputFields[info.id] = []byte("false")

				case "string":
					outputFields[info.id] = []byte("")

				default:
					outputFields[info.id] = []byte("")
				}
			}
		}
	}

	// adjust END
	if end, exists := outputFields["END"]; exists {
		if end.(int32) == 0 {
			outputFields["END"] = int32(v.End)
		}
	}

	return outputFields
}

// SVtype string // Structural Variant type
// IsSV   bool   // Is Structural Variant (CNV, INS, INV, BND)

// // Only applies to inversions and breakends
// TranslocatedChr         string
// TranslocatedStart       int
// TranslocatedIsPosStrand bool
// TranslocatedComesAfter  bool
