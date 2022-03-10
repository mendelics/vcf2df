package vcfio

import (
	"log"
	"strconv"
	"strings"
)

// ParseVariant is the entry point for VCF parsing
func ParseVariant(line string, header *Header) (VariantInfo, Quality, []SampleSpecific, ControlDB, string) {
	fields := strings.Split(line, "\t")

	// Multiple alts
	if len(strings.Split(string(fields[4]), ",")) > 1 {
		log.Printf("VCF file must be denormalized with bcftools norm -m -any: %s\n", string(line))
	}

	// VCF 4.3 spec: the ‘*’ allele is reserved to indicate that the allele is missing due to an overlapping deletion.
	// The variant appears in 2 consecutive lines. We remove these * alt variants.
	if string(fields[4]) == "*" {
		return VariantInfo{Alt: "*"}, Quality{}, nil, ControlDB{}, ""
	}

	pos, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		log.Println(err)
	}

	chr := fields[0]                  // Chr
	start := int(pos) - 1             // Start
	ref := strings.ToUpper(fields[3]) // Ref
	alt := fields[4]                  // Alt

	// INFO
	info := NewInfoByte([]byte(fields[7]), header)

	// Parse VCF
	variantInfo, err := extractVcfFields(chr, start, ref, alt, info)
	if err != nil {
		log.Fatalf("Error extracting vcf INFO, %v\n", err)
	}

	// Quality and reads parameters
	quality, err := extractVcfQUAL(fields, info)
	if err != nil {
		log.Fatalf("Error extracting vcf QUAL, %v\n", err)
	}

	// Genotypes for each sample in VCF
	genotypes, err := extractVcfFORMAT(fields, info, header.SampleNames, variantInfo.SVtype)
	if err != nil {
		log.Println(line)
		log.Fatalf("Error extracting vcf FORMAT, %v\n", err)
	}

	// MAF (GnomAD) and label info
	mafs, label, err := extractVcfMAFS(chr, info)
	if err != nil {
		log.Fatalf("Error extracting vcf MAFS, %v\n", err)
	}

	return variantInfo, quality, genotypes, mafs, label
}
