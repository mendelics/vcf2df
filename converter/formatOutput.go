package converter

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mendelics/vcfio"
)

func formatOutputMap(
	v vcfio.VariantInfo,
	q vcfio.Quality,
	g vcfio.SampleSpecific,
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
		"QUAL":       int32(q.QualScore),
		"FILTER":     (q.Filter == "PASS" || q.Filter == "."),
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
