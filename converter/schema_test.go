package converter

import (
	"testing"

	"github.com/mendelics/vcfio"
)

// TestDefineSchemaMsg - func defineSchemaMsg(infos map[string]*vcfio.InfoVcf) (string, []infoField, error) {
func TestDefineSchemaMsg(t *testing.T) {
	tt := []struct {
		infoHeaderMap   map[string]*vcfio.InfoVcf
		expectedMessage string
	}{
		{
			map[string]*vcfio.InfoVcf{
				// "##INFO=<ID=AC,Number=A,Type=Integer,Description=\"Allele count in genotypes, for each ALT allele, in the same order as listed\">",
				"AC": {
					Id:          "AC",
					Description: "Allele count in genotypes, for each ALT allele, in the same order as listed",
					Number:      "A",
					Type:        "Integer",
				},
				// "##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">",
				"AF": {
					Id:          "AF",
					Description: "Allele Frequency, for each ALT allele, in the same order as listed",
					Number:      "A",
					Type:        "Float",
				},
				// "##INFO=<ID=DS,Number=0,Type=Flag,Description=\"Were any of the samples downsampled?\">",
				"DS": {
					Id:          "DS",
					Description: "Were any of the samples downsampled?",
					Number:      "0",
					Type:        "Flag",
				},
				// "##INFO=<ID=AS_RAW_MQ,Number=1,Type=String,Description=\"Allele-specfic raw data for RMS Mapping Quality\">",
				"AS_RAW_MQ": {
					Id:          "AS_RAW_MQ",
					Description: "Allele-specfic raw data for RMS Mapping Quality",
					Number:      "1",
					Type:        "String",
				},
			},
			"message test {required binary VARIANTKEY (STRING); required binary CHROM (STRING); required int32 POS; required binary REF (STRING); required binary ALT (STRING); required double QUAL; required binary FILTER (STRING); required boolean IS_SV; required binary SVTYPE (STRING); required int32 END; required int32 NUMALTS; required binary SAMPLE (STRING); required boolean IS_PHASED; required binary PHASE_ID (STRING); required binary AC (STRING); required binary AF (STRING); required binary AS_RAW_MQ (STRING); required boolean DS;}",
		},
		{
			map[string]*vcfio.InfoVcf{},
			"message test {required binary VARIANTKEY (STRING); required binary CHROM (STRING); required int32 POS; required binary REF (STRING); required binary ALT (STRING); required double QUAL; required binary FILTER (STRING); required boolean IS_SV; required binary SVTYPE (STRING); required int32 END; required int32 NUMALTS; required binary SAMPLE (STRING); required boolean IS_PHASED; required binary PHASE_ID (STRING);}",
		},
	}
	for i, test := range tt {
		header := vcfio.Header{
			Infos: test.infoHeaderMap,
		}
		obtainedMessage, _, _ := defineSchemaMessage(&header)
		if obtainedMessage != test.expectedMessage {
			t.Errorf("Test number %d:\nexpected message:\n%s\nobtained message:\n%s\n", i, test.expectedMessage, obtainedMessage)
		}
	}
}
