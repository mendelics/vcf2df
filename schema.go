package main

import (
	"strings"

	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/mendelics/vcfio"
)

func defineSchema(numaltsColumnName string, outputAllVcfColumns bool) (*parquetschema.SchemaDefinition, error) {
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

	msgStr = strings.Replace(msgStr, "numalts", numaltsColumnName, -1)

	return parquetschema.ParseSchemaDefinition(msgStr)
}

func formatOutputMap(numaltsColumnName string, outputAllVcfColumns bool, v vcfio.VariantInfo, q vcfio.Quality, g []vcfio.SampleSpecific, info string) map[string]interface{} {
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

// func defineSchema(outputFilename string, outputAllVcfColumns bool, numaltsColumnName string, header *vcfio.Header) (*parquetschema.SchemaDefinition, []infoField, error) {
// 	var msgStrStart string
// 	// msgStrEnd := "}"

// 	switch {
// 	case outputAllVcfColumns:
// 		msgStrStart = `message test {
// 			required binary variantkey (STRING);
// 			required binary chrom (STRING);
// 			required int32 pos;
// 			required binary ref (STRING);
// 			required binary alt (STRING);
// 			required double qual;
// 			required binary filter (STRING);
// 			required binary info (STRING);
// 			required int32 #NUMALTS#;
// 		}`

// 	default:
// 		msgStrStart = `message test {
// 		required binary variantkey (STRING);
// 		required int32 #NUMALTS#;
// 	}`
// 	}

// 	msgStrStart = strings.Replace(msgStrStart, "#NUMALTS#", outputFilename, -1)

// 	// infos := make([]string, 0)
// 	infoList := make([]infoField, 0)
// 	// for _, info := range header.Infos {
// 	// 	var line string
// 	// 	if info.Number == "1" {
// 	// 		switch {
// 	// 		case info.Type == "Integer":
// 	// 			line = fmt.Sprintf("required int32 %s;\n", info.Id)
// 	// 			infoList = append(infoList, infoField{id: info.Id, infoType: "int32"})
// 	// 		case info.Type == "Float":
// 	// 			line = fmt.Sprintf("required double %s;\n", info.Id)
// 	// 			infoList = append(infoList, infoField{id: info.Id, infoType: "float64"})
// 	// 		case info.Type == "Flag":
// 	// 			line = fmt.Sprintf("required boolean %s;\n", info.Id)
// 	// 			infoList = append(infoList, infoField{id: info.Id, infoType: "bool"})
// 	// 		default:
// 	// 			line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
// 	// 			infoList = append(infoList, infoField{id: info.Id, infoType: "string"})
// 	// 		}
// 	// 	} else {
// 	// 		line = fmt.Sprintf("required binary %s (STRING);\n", info.Id)
// 	// 		infoList = append(infoList, infoField{id: info.Id, infoType: "string"})
// 	// 	}

// 	// 	infos = append(infos, line)
// 	// }

// 	// infoStr := strings.Join(infos, "")
// 	// msg := fmt.Sprintf("%s%s%s", msgStrStart, infoStr, msgStrEnd)

// 	schemadef, err := parquetschema.ParseSchemaDefinition(msgStrStart)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	return schemadef, infoList, nil
// }

// func formatOutputMap(
// 	numaltsColumnName string,
// 	outputAllVcfColumns bool,
// 	v vcfio.VariantInfo,
// 	q vcfio.Quality,
// 	g []vcfio.SampleSpecific,
// 	infoList []infoField,
// 	infos *vcfio.InfoByte,
// ) map[string]interface{} {

// 	var numalts int32
// 	if len(g) != 0 {
// 		numalts = int32(g[0].NumAlts)
// 	}

// 	switch {
// 	case outputAllVcfColumns:
// 		outputFields := map[string]interface{}{
// 			"variantkey":      []byte(v.VariantKey),
// 			"chrom":           []byte(v.Chr),
// 			"pos":             int32(v.Start + 1),
// 			"ref":             []byte(v.Ref),
// 			"alt":             []byte(v.Alt),
// 			"qual":            q.QualScore,
// 			"filter":          []byte(q.Filter),
// 			numaltsColumnName: numalts,
// 		}

// 		// for _, info := range infoList {
// 		// 	valueInterface, err := infos.Get(info.id)
// 		// 	if err == nil && valueInterface != nil {
// 		// 		outputFields[info.id] = valueInterface
// 		// 		// switch info.infoType {
// 		// 		// case "int32":
// 		// 		// 	value := valueInterface.(int32)
// 		// 		// 	outputFields[info.id] = value
// 		// 		// case "float64":
// 		// 		// 	value := valueInterface.(float64)
// 		// 		// 	outputFields[info.id] = value
// 		// 		// case "bool":
// 		// 		// 	value := valueInterface.(bool)
// 		// 		// 	outputFields[info.id] = value
// 		// 		// default:
// 		// 		// 	value := valueInterface.(string)
// 		// 		// 	outputFields[info.id] = []byte(value)
// 		// 		// }
// 		// 		// } else {
// 		// 		// 	outputFields[info.id] =
// 		// 		// 	switch info.infoType {
// 		// 		// 	case "int32":
// 		// 		// 		outputFields[info.id] = int32(0)
// 		// 		// 	case "float64":
// 		// 		// 		outputFields[info.id] = 0.0
// 		// 		// 	case "bool":
// 		// 		// 		outputFields[info.id] = false
// 		// 		// 	default:
// 		// 		// 		outputFields[info.id] = []byte("")
// 		// 		// 	}
// 		// 	}
// 		// }

// 		return outputFields

// 	default:
// 		return map[string]interface{}{
// 			"variantkey":      []byte(v.VariantKey),
// 			numaltsColumnName: numalts,
// 		}
// 	}
// }
