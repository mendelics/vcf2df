package converter

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/mendelics/vcfio"
)

type infoField struct {
	id          string
	infoType    string
	Description string
	Rootid      string
	Position    int
	TotalValues int
	IsSlice     bool
}

func defineSchemaMessage(header *vcfio.Header) (string, []infoField, error) {
	schemaSlice := make([]string, 0)
	infoList := make([]infoField, 0)

	schemaSlice = append(schemaSlice, []string{
		"required binary VARIANTKEY (STRING)",
		"required binary CHROM (STRING)",
		"required int32 POS",
		"required binary REF (STRING)",
		"required binary ALT (STRING)",
		"required int32 QUAL",
		"required binary FILTER (STRING)",
		"required binary SAMPLE (STRING)",
		"required int32 NUMALTS",
		"required boolean IS_PHASED",
		"required binary PHASE_ID (STRING)",
		"required boolean PASS",
		"required boolean IS_SV",
		"required binary SVTYPE (STRING)",
		"required int32 END",
	}...)

	infoSlice := make([]string, 0)
	// INFO
	for _, info := range header.Infos {
		if reservedColumnNames[info.Id] {
			continue
		}

		infoNum, isSlice, err := numberStr2Int(info.Number, len(header.SampleNames))
		if err != nil {
			return "", infoList, err
		}

		var line string
		var infoTypeStr string
		var infoDescription string

		for i := 0; i < infoNum; i++ {
			infoID := info.Id
			if i != 0 {
				infoID = fmt.Sprintf("%s_%d", info.Id, i)
			}

			switch {
			case info.Type == "Integer":
				line = fmt.Sprintf("required int32 %s", infoID)
				infoTypeStr = "int32"
				infoDescription = info.Description

			case info.Type == "Float":
				line = fmt.Sprintf("required double %s", infoID)
				infoTypeStr = "float"
				infoDescription = info.Description

			case info.Type == "Flag":
				line = fmt.Sprintf("required boolean %s", infoID)
				infoTypeStr = "bool"
				infoDescription = info.Description

			case info.Type == "String":
				line = fmt.Sprintf("required binary %s (STRING)", infoID)
				infoTypeStr = "string"
				infoDescription = info.Description

			default:
				continue
			}

			infoList = append(infoList, infoField{
				id:          infoID,
				infoType:    infoTypeStr,
				Description: infoDescription,
				Rootid:      info.Id,
				Position:    i,
				TotalValues: infoNum,
				IsSlice:     isSlice,
			})
		}

		infoSlice = append(infoSlice, line)
	}

	// Guarantees same order of columns, therefore is testable
	sort.Strings(infoSlice)
	schemaSlice = append(schemaSlice, infoSlice...)

	schemaStr := strings.Join(schemaSlice, "; ")
	msg := fmt.Sprintf("message test {%s;}", schemaStr)

	return msg, infoList, nil
}

func numberStr2Int(numStr string, genotypeNum int) (int, bool, error) {
	switch numStr {
	case "A":
		return 1, true, nil
	case "R":
		return 2, true, nil
	case "G":
		return genotypeNum, true, nil
	case ".":
		return 1, true, nil
	case "":
		return 1, true, nil
	case "0":
		return 1, false, nil
	}

	numInt, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, false, err
	}

	return numInt, (numInt != 1), nil
}
