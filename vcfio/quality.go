package vcfio

import (
	"errors"
	"log"
	"math"
	"strconv"
)

// extractVcfQUAL parses information about variant quality
func extractVcfQUAL(fields []string, info *InfoByte) (Quality, error) {
	// Variant quality in this instance comes from fields 5 and 6
	qual := Quality{}
	var err error

	// When VCF comes from Mutect and not GATK, this field is "."
	if string(fields[5]) != "." {
		qual.QualScore, err = strconv.ParseFloat(fields[5], 64)
		if err != nil {
			log.Printf("error parsing fields[5]: %v", fields[5])
			return qual, errors.New(" err parsing quality score")
		}
	}

	qual.Filter = fields[6]

	dpTot, err := info.Get("DP")
	if err == nil && dpTot != nil {
		dp := dpTot.(int)
		qual.ReadDepth = &dp
	}

	dpAlt, err := info.Get("_AB")
	var dpA int
	if err == nil && dpAlt != nil {
		dpA = dpAlt.(int)
	}

	if qual.ReadDepth != nil {
		readRef := *qual.ReadDepth - int(math.Round(float64(*qual.ReadDepth*dpA)/100))
		qual.ReadDepthRef = &readRef

		readAlt := int(math.Round(float64(*qual.ReadDepth*dpA) / 100))
		qual.ReadDepthAlt = &readAlt
	}

	numSnp, err := info.Get("NUMSNP")
	if err == nil && numSnp != nil {
		snp := numSnp.(int)
		qual.SnpCount = snp
	}

	return qual, nil
}
