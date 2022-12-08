package converter

var reservedColumnNames = map[string]bool{
	"VARIANTKEY": true, // New columns always added by vcf2df
	"CHROM":      true, // VCF spec
	"POS":        true, // VCF spec
	"REF":        true, // VCF spec
	"ALT":        true, // VCF spec
	"QUAL":       true, // VCF spec
	"FILTER":     true, // VCF spec
	"SAMPLE":     true, // Denormalized per sample
	"NUMALTS":    true, // Denormalized per sample
	"IS_PHASED":  true, // Phasing
	"PHASE_ID":   true, // Phasing
	"PASS":       true, // New columns always added by vcf2df
	"IS_SV":      true, // Structural variant
	"SVTYPE":     true, // Structural variant
	"END":        true, // Structural variant
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
		"CHROM":      "Chromosome (including chr preffix).",
		"POS":        "Position (1-based)",
		"REF":        "Reference allele. Empty for structural variants.",
		"ALT":        "Alternate allele. Empty for structural variants.",
		"QUAL":       "Quality score (Integer)",
		"FILTER":     "Filter string",
		"SAMPLE":     "Sample string",
		"NUMALTS":    "Number of alternate alleles (0, 1, 2)",
		"IS_PHASED":  "Boolean if variant is phased.",
		"PHASE_ID":   "String identifying variant phase.",
		"VARIANTKEY": "Variantkey is CHR-POS-REF-ALT for small variants and CHR-POS-END-SVTYPE for structural variants. CHR without chr preffix.",
		"PASS":       "Boolean describing filter == PASS || filter == .",
		"IS_SV":      "Boolean structural variant.",
		"SVTYPE":     "Structural variant type (ex. DEL, DUP, INV, ...)",
		"END":        "End position of variant (1-based).",
	}

	for _, info := range infoList {
		columns[info.id] = info.Description
	}

	return columns
}
