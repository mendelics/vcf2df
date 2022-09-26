package converter

var reservedColumnNames = map[string]bool{
	"VARIANTKEY": true,
	"CHROM":      true,
	"POS":        true,
	"REF":        true,
	"ALT":        true,
	"QUAL":       true,
	"PASS":       true,
	"IS_SV":      true,
	"SVTYPE":     true,
	"END":        true,
	"NUMALTS":    true,
	"SAMPLE":     true,
	"IS_PHASED":  true,
	"PHASE_ID":   true,
	"REF_READS":  true,
	"ALT_READS":  true,
}

// TODO: ADD REPEATS (ex. EXPANSION HUNTER)
// Repeat infos

// TODO: ADD COMPLEX STRUCTURAL VARIANT DATA
// TranslocatedChr         string
// TranslocatedStart       int
// TranslocatedIsPosStrand bool
// TranslocatedComesAfter  bool

func mapColumnNames(infoList []infoField) map[string]string {
	columns := map[string]string{
		"VARIANTKEY": "Variantkey is CHR-POS-REF-ALT for small variants and CHR-POS-END-SVTYPE for structural variants. CHR without chr preffix.",
		"CHROM":      "Chromosome (including chr preffix).",
		"POS":        "Position (1-based)",
		"REF":        "Reference allele. Empty for structural variants.",
		"ALT":        "Alternate allele. Empty for structural variants.",
		"QUAL":       "Quality score (Integer)",
		"PASS":       "Boolean describing filter == PASS || filter == .",
		"IS_SV":      "Boolean structural variant.",
		"SVTYPE":     "Structural variant type (ex. DEL, DUP, INV, ...)",
		"END":        "End position of variant (1-based).",
		"NUMALTS":    "Number of alternate alleles (0, 1, 2)",
		"SAMPLE":     "Sample string",
		"IS_PHASED":  "Boolean if variant is phased.",
		"PHASE_ID":   "String identifying variant phase.",
		"REF_READS":  "Read depth for ref",
		"ALT_READS":  "Read depth for alt",
	}

	for _, info := range infoList {
		columns[info.id] = info.Description
	}

	return columns
}
