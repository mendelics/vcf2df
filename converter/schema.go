package converter

import (
	"fmt"
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
