package converter

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
