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

func defineSchema(header *vcfio.Header, outputFilename string) (*parquetschema.SchemaDefinition, []infoField, error) {
	schemaSlice := make([]string, 0)
	infoList := make([]infoField, 0)

	schemaSlice = append(schemaSlice, []string{
		"required binary VARIANTKEY (STRING)",
		"required binary CHROM (STRING)",
		"required int32 POS",
		"required binary REF (STRING)",
		"required binary ALT (STRING)",
		"required double QUAL",
		"required binary FILTER (STRING)",
		"required boolean IS_SV",
		"required binary SVTYPE (STRING)",
		"required int32 END",
		"required int32 NUMALTS",
		"required binary SAMPLE (STRING)",
		"required boolean IS_PHASED",
		"required binary PHASE_ID (STRING)",
	}...)

	// INFO
	for _, info := range header.Infos {
		if reservedColumnNames[info.Id] {
			continue
		}

		var line string
		var infoTypeStr string

		switch {
		case info.Type == "Integer" && info.Number == "1":
			line = fmt.Sprintf("required int32 %s", info.Id)
			infoTypeStr = "int32"

		case info.Type == "Float" && info.Number == "1":
			line = fmt.Sprintf("required double %s", info.Id)
			infoTypeStr = "float64"

		case info.Type == "Flag" && info.Number == "0":
			line = fmt.Sprintf("required boolean %s", info.Id)
			infoTypeStr = "bool"

		case info.Type == "String" && info.Number == "1":
			line = fmt.Sprintf("required binary %s (STRING)", info.Id)
			infoTypeStr = "string"

		case info.Number != "1":

			switch {
			case info.Type == "Integer":
				line = fmt.Sprintf("required binary %s (STRING)", info.Id)
				infoTypeStr = "[]int"

			case info.Type == "Float":
				line = fmt.Sprintf("required binary %s (STRING)", info.Id)
				infoTypeStr = "[]float32"
			}

		default:
			continue
		}

		infoList = append(infoList, infoField{
			id:       info.Id,
			infoType: infoTypeStr,
		})

		schemaSlice = append(schemaSlice, line)
	}

	schemaStr := strings.Join(schemaSlice, ";\n")
	msg := fmt.Sprintf("message test {\n%s;\n}", schemaStr)

	schemadef, err := parquetschema.ParseSchemaDefinition(msg)
	if err != nil {
		return nil, nil, err
	}

	return schemadef, infoList, nil
}

func formatOutputMap(
	v vcfio.VariantInfo,
	q vcfio.Quality,
	g vcfio.SampleSpecific,
	infoList []infoField,
	infos *vcfio.InfoByte,
	outputFilename string,
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
		"IS_SV":      v.IsSV,
		"SVTYPE":     []byte(v.SVtype),
		"END":        int32(v.End),
		"NUMALTS":    int32(g.NumAlts),
		"SAMPLE":     []byte(g.SampleName),
		"IS_PHASED":  g.IsPhased,
		"PHASE_ID":   []byte(g.PhaseID),
	}

	// outputAllVcfColumns iterating over INFO fields
	for _, info := range infoList {
		valueInterface, err := infos.Get(info.id)

		if err == nil && valueInterface != nil {
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

	return outputFields
}

var reservedColumnNames = map[string]bool{
	"VARIANTKEY": true,
	"CHROM":      true,
	"POS":        true,
	"REF":        true,
	"ALT":        true,
	"QUAL":       true,
	"FILTER":     true,
	"IS_SV":      true,
	"SVTYPE":     true,
	"END":        true,
	"NUMALTS":    true,
	"SAMPLE":     true,
	"IS_PHASED":  true,
	"PHASE_ID":   true,
}
