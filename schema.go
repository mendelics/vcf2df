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

func defineSchema(numaltsColumnName, selectedINFO string, useSampleAsColumn, outputAllVcfColumns bool, header *vcfio.Header) (*parquetschema.SchemaDefinition, []infoField, error) {
	// var msgStr string
	var msgStrStart string
	msgStrEnd := "}"

	switch {
	case outputAllVcfColumns:
		msgStrStart = `message test {
			required binary variantkey (STRING);
			required binary chrom (STRING);
			required int32 pos;
			required binary ref (STRING);
			required binary alt (STRING);
			required double qual;
			required binary filter (STRING);
			required int32 numalts;
`

	case selectedINFO != "":
		msgStrStart = `message test {
		required binary variantkey (STRING);
`

	default:
		msgStrStart = `message test {
		required binary variantkey (STRING);
		required int32 numalts;
`
	}

	msgStrStart = strings.Replace(msgStrStart, "numalts", numaltsColumnName, -1)

	infos := make([]string, 0)
	infoList := make([]infoField, 0)

	for _, info := range header.Infos {
		if selectedINFO != "" && selectedINFO != info.Id {
			continue
		}

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

		if selectedINFO != "" && useSampleAsColumn {
			line = strings.Replace(line, selectedINFO, numaltsColumnName, -1)
		}

		infos = append(infos, line)
	}

	infoStr := strings.Join(infos, "")
	msg := fmt.Sprintf("%s%s%s", msgStrStart, infoStr, msgStrEnd)

	schemadef, err := parquetschema.ParseSchemaDefinition(msg)
	if err != nil {
		return nil, nil, err
	}

	return schemadef, infoList, nil
}

func formatOutputMap(
	numaltsColumnName string,
	selectedINFO string,
	useSampleAsColumn bool,
	outputAllVcfColumns bool,
	v vcfio.VariantInfo,
	q vcfio.Quality,
	g []vcfio.SampleSpecific,
	infoList []infoField,
	infos *vcfio.InfoByte,
) map[string]interface{} {

	var numalts int32
	if len(g) != 0 {
		numalts = int32(g[0].NumAlts)
	}

	outputFields := map[string]interface{}{
		"variantkey": []byte(v.VariantKey),
	}

	switch {
	case outputAllVcfColumns:
		outputFields["chrom"] = []byte(v.Chr)
		outputFields["pos"] = int32(v.Start + 1)
		outputFields["ref"] = []byte(v.Ref)
		outputFields["alt"] = []byte(v.Alt)
		outputFields["qual"] = q.QualScore
		outputFields["filter"] = []byte(q.Filter)
		outputFields[numaltsColumnName] = numalts

	case selectedINFO != "":
		// skip to infoList (which should be only one)

	default:
		outputFields[numaltsColumnName] = numalts
		return outputFields
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

	// Change selected INFO field name to filename (used for PRS BETA)
	if selectedINFO != "" && numaltsColumnName != "numalts" {
		outputFields[numaltsColumnName] = outputFields[selectedINFO]
		delete(outputFields, selectedINFO)
	}

	return outputFields
}
