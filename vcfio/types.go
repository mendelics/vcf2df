package vcfio

// VariantInfo is the replicated variant identifying information
type VariantInfo struct {
	VariantKey string
	Chr        string
	Start      int
	End        int
	Ref        string
	Alt        string

	// Structural Variant
	SVtype string // Structural Variant type
	IsSV   bool   // Is Structural Variant (CNV, INS, INV, BND)

	// Only applies to inversions and breakends
	TranslocatedChr         string
	TranslocatedStart       int
	TranslocatedIsPosStrand bool
	TranslocatedComesAfter  bool
}

// Quality of the mapping and genotype calling in that instance
type Quality struct {
	QualScore float64
	Filter    string // "PASS" or "ABlow;FShigh"

	// Reads
	ReadDepth    *int
	ReadDepthRef *int
	ReadDepthAlt *int

	Platform string // currently "array" || ""
	SnpCount int    // CNVs called from this number of SNPs in array
}

// SampleSpecific is the information of each sample's variants //GT:AD:GQ:PL	1/1:0,16:48:559,48,0
type SampleSpecific struct {
	SampleName string //
	NumAlts    int    // number of alts (het = 1, hom = 2)

	// Phasing
	IsPhased bool   // "/" = false, "|" = true
	PhaseID  string //

	// Reads
	ReadDepthRef int //
	ReadDepthAlt int //

	// Trios
	Paternal  bool // Paternally inherited
	Maternal  bool // Maternally inherited
	Isodisomy bool // Both alleles inherited from same parent
	Denovo    bool // De novo variant
}

// ControlDB is the collection of GnomAD and other controls
type ControlDB struct {
	Gnomad ControlInfo // Variant frequency and allele counts
}

// ControlInfo contains the GnomAD AFs
type ControlInfo struct {
	AlleleCount      int
	AlleleTotal      int
	AlleleCountMale  int
	AlleleTotalMale  int
	HomoCount        int
	HetCount         int
	AlleleFreqPopMax float64
	Start            int    // GnomAD SV
	End              int    // GnomAD SV
	SVtype           string // GnomAD SV
}
