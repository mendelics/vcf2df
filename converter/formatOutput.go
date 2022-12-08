package converter

import (
	"log"

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
		"FILTER":     []byte(q.Filter),
		"SAMPLE":     []byte(g.SampleName),
		"NUMALTS":    int32(g.NumAlts),
		"IS_PHASED":  g.IsPhased,
		"PHASE_ID":   []byte(g.PhaseID),
		"PASS":       (q.Filter == "PASS" || q.Filter == "."),
		"IS_SV":      v.IsSV,
		"SVTYPE":     []byte(v.SVtype),
		"END":        int32(v.End),
	}

	// outputAllVcfColumns iterating over INFO fields
	for _, info := range infoList {
		h, err := infos.Get(info.Rootid)

		if err == nil && h != nil {
			switch info.infoType {
			case "int32":
				if info.TotalValues == 1 && !info.IsSlice {
					value := h.(int)
					outputFields[info.id] = int32(value)
				} else {
					values := h.([]int)
					if len(values) != info.TotalValues {
						log.Fatalf("Problem with %+v and %+v", h, info)
					}
					value := values[info.Position]
					outputFields[info.id] = int32(value)
				}

			case "float":
				if info.TotalValues == 1 && !info.IsSlice {
					value := h.(float64)
					outputFields[info.id] = value
				} else {
					values := h.([]float32)
					if len(values) != info.TotalValues {
						log.Fatalf("Problem with %+v and %+v", h, info)
					}
					value := values[info.Position]
					outputFields[info.id] = float64(value)
				}

			case "bool":
				outputFields[info.id] = true

			case "string":
				value := h.(string)
				outputFields[info.id] = []byte(value)

			default:
				continue
			}
		} else {

			switch info.infoType {
			case "int32":
				outputFields[info.id] = int32(0)

			case "float":
				outputFields[info.id] = float64(0.0)

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
