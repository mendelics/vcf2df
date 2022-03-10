package vcfio

import (
	"errors"
	"log"
	"strconv"
	"strings"
)

// extractVcfFORMAT parses genotyping information in latter columns
func extractVcfFORMAT(fields []string, info *InfoByte, sampleNames []string, svtype string) ([]SampleSpecific, error) {
	// Samples parsing (column 8 for FORMAT, and 9+ for sample genotypes)
	genotypes := []SampleSpecific{}

	if len(fields) < 9 {
		return genotypes, nil
	}

	samples := fields[9:]
	format := strings.Split(fields[8], ":")

	for i, sample := range samples {

		// Parse genotype line
		gtMap := createGtMap(format, sample)

		gt, err := parseSampleGenotypes(gtMap)
		if err != nil {
			return genotypes, errors.New("err parsing sample genotypes")
		}

		// Add sample name to genotype
		if len(sampleNames) == len(samples) {
			gt.SampleName = sampleNames[i]
		} else {
			return genotypes, errors.New("samples in header different from samples in vcf lines")
		}

		cn, cnErr := info.Get("CN")
		if cnErr != nil && !strings.Contains(cnErr.Error(), "not found in header") {
			log.Println("Error getting CN from INFO", info, cnErr)
		} else if cn != nil {
			cnNum, err := strconv.Atoi(cn.(string))
			if err == nil {
				switch {
				case cnNum == 2:
					gt.NumAlts = 0
				case cnNum == 1:
					gt.NumAlts = 1
				case cnNum == 0:
					gt.NumAlts = 2
				default:
					gt.NumAlts = cnNum - 2
				}
			}
		}

		if svtype == "LOH" {
			gt.NumAlts = 1
		}

		genotypes = append(genotypes, gt)
	}

	return genotypes, nil
}

func createGtMap(format []string, unparsedSample string) map[string]string {
	gtMap := make(map[string]string)

	sampleFields := strings.Split(unparsedSample, ":") // 1/1:0,56:99:1888,167,0 -> 1/1	0,56	99	1888,167,0

	for i, ft := range format {
		gtMap[ft] = sampleFields[i]
	}

	return gtMap
}

func parseSampleGenotypes(gtMap map[string]string) (gt SampleSpecific, err error) {
	// Prefer phased variants (PGT), but accept unphased (GT)
	if field, exists := gtMap["PGT"]; exists {
		if strings.Contains(field, "|") {
			gt.IsPhased = true
		}

		switch {
		case field == "0/0" || field == "0|0" || field == ".|." || field == "./.":
			gt.NumAlts = 0

		case field == "0/1" || field == "0|1" || field == "1/0" || field == "1|0":
			gt.NumAlts = 1

		case field == "1/1" || field == "1|1":
			gt.NumAlts = 2
		}
	} else if field, exists := gtMap["GT"]; exists {
		if strings.Contains(field, "|") {
			gt.IsPhased = true
		}

		switch {
		case field == "0/0" || field == "0|0" || field == ".|." || field == "./.":
			gt.NumAlts = 0

		case field == "0/1" || field == "0|1" || field == "1/0" || field == "1|0":
			gt.NumAlts = 1

		case field == "1/1" || field == "1|1":
			gt.NumAlts = 2
		}
	}

	// PID Phase ID identifies variants in same strand
	if field, exists := gtMap["PID"]; exists {
		gt.PhaseID = field
	}

	// AD R Integer Read depth for each allele
	if field, exists := gtMap["AD"]; exists {
		ad := strings.Split(field, ",")

		// Avoids out of bounds error if AD isn't int,int
		if len(ad) == 2 {

			if ad[0] == "." {
				gt.ReadDepthRef = 0
			} else {
				gt.ReadDepthRef, err = strconv.Atoi(ad[0])
				if err != nil {
					return gt, err
				}
			}

			if ad[1] == "." {
				gt.ReadDepthAlt = 0
			} else {
				gt.ReadDepthAlt, err = strconv.Atoi(ad[1])
				if err != nil {
					return gt, err
				}
			}
		}
	}

	return gt, nil
}
