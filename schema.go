package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/mendelics/vcfio"
)

type infoField struct {
	id       string
	infoType string
}

func defineSchema(header *vcfio.Header) (*parquetschema.SchemaDefinition, []infoField, error) {
	msgStrStart := `message test {
			required binary VARIANTKEY (STRING);
			required binary CHROM (STRING);
			required int32 POS;
			required binary REF (STRING);
			required binary ALT (STRING);
			required double QUAL;
			required binary FILTER (STRING);
`
	samplesSchemaLines := make([]string, 0)
	for _, sample := range header.SampleNames {
		sampleLine := fmt.Sprintf("required int32 NUMALTS@%s;\n", sample)
		samplesSchemaLines = append(samplesSchemaLines, sampleLine)
	}

	infos := make([]string, 0)
	infoList := make([]infoField, 0)

	for _, info := range header.Infos {
		var line string

		switch {
		case info.Type == "Integer" && info.Number == "1":
			line = fmt.Sprintf("required int32 %s;\n", info.Id)
			infoList = append(infoList, infoField{id: info.Id, infoType: "int32"})

		case info.Type == "Float" && info.Number == "1":
			line = fmt.Sprintf("required double %s;\n", info.Id)
			infoList = append(infoList, infoField{id: info.Id, infoType: "float64"})

		case info.Type == "Flag" && info.Number == "0":
			line = fmt.Sprintf("required boolean %s;\n", info.Id)
			infoList = append(infoList, infoField{id: info.Id, infoType: "bool"})

		case info.Type == "String" && info.Number == "1":
			line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
			infoList = append(infoList, infoField{id: info.Id, infoType: "string"})

		case info.Number != "1":

			switch {
			case info.Type == "Integer":
				line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
				infoList = append(infoList, infoField{id: info.Id, infoType: "[]int"})

			case info.Type == "Float":
				line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
				infoList = append(infoList, infoField{id: info.Id, infoType: "[]float32"})
			}

		default:
			continue
		}

		infos = append(infos, line)
	}

	samplesStr := strings.Join(samplesSchemaLines, "")
	infoStr := strings.Join(infos, "")
	msg := fmt.Sprintf("%s%s%s}", msgStrStart, samplesStr, infoStr)

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
) map[string]interface{} {

	// Every df contains variantkey
	outputFields := map[string]interface{}{
		"VARIANTKEY": []byte(v.VariantKey),
		"CHROM":      []byte(v.Chr),
		"POS":        int32(v.Start + 1),
		"REF":        []byte(v.Ref),
		"ALT":        []byte(v.Alt),
		"QUAL":       q.QualScore,
		"FILTER":     []byte(q.Filter),
	}

	for _, sample := range g {
		columnName := fmt.Sprintf("NUMALTS@%s", sample.SampleName)
		outputFields[columnName] = int32(sample.NumAlts)
	}

	// outputAllVcfColumns iterating over INFO fields
	for _, info := range infoList {
		valueInterface, err := infos.Get(info.id)

		if err == nil && valueInterface != nil {
			outputFields[info.id] = valueInterface

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
