package vcfio

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/biogo/hts/bgzf"
)

// ReadVcf reads VCF file into stream for irelate
func ReadVcf(vcfFile string) (*VcfReader, error) {
	// Open VCF file
	rdr, err := ioutil.ReadFile(vcfFile)
	if err != nil {
		return nil, err
	}

	byteRdr := bytes.NewReader(rdr)

	// Presumes VCF with denormalized variants ("bcftools norm -m -any"), bgzipped and tabix indexed
	qrdr, err := bgzf.NewReader(byteRdr, 0)
	if err != nil {
		newErr := fmt.Errorf("error opening query file %s: %s", vcfFile, err)
		return nil, newErr
	}

	// Parse VCF file with vcfgo to use in irelate
	vcfRdr, err := NewVcfReader(qrdr)
	if err != nil {
		log.Fatalf("error parsing VCF query file %s: %v", vcfFile, err)
	}

	return vcfRdr, nil
}

// ReadNewVcf returns a Scanner to iterate over VCF records and the parsed Header.
// It assumes denormalized variants (bcftools norm -m -any)
func ReadNewVcf(vcfFile string) (*bufio.Scanner, *Header, error) {
	// Open VCF file
	rdr, err := ioutil.ReadFile(vcfFile)
	if err != nil {
		return nil, nil, err
	}

	byteRdr := bytes.NewReader(rdr)
	bgzipReader, err := bgzf.NewReader(byteRdr, 0)
	if err != nil {
		newErr := fmt.Errorf("error opening query file %s: %s", vcfFile, err)
		return nil, nil, newErr
	}

	s := bufio.NewScanner(bgzipReader)
	header, err := parseHeader(s)
	if err != nil {
		return nil, nil, err
	}
	return s, header, nil
}

// parseHeader will read the scanner lines until it reaches
// the '#CHROM' line. Every conditional aims to parse specific
// information from the VCF header.
func parseHeader(s *bufio.Scanner) (*Header, error) {
	header := NewHeader()

	for s.Scan() {
		line := s.Text()

		if strings.HasPrefix(line, "##fileformat") {
			v, err := parseHeaderFileVersion(line)
			if err != nil {
				log.Println(err)
			}
			header.FileFormat = v

		} else if strings.HasPrefix(line, "##FORMAT") {
			format, err := parseHeaderFormat(line)
			if err != nil {
				log.Println(err)
			}
			if format != nil {
				header.SampleFormats[format.Id] = format
			}

		} else if strings.HasPrefix(line, "##INFO") {
			info, err := parseHeaderInfo(line)
			if err != nil {
				log.Println(err)
			}
			if info != nil {
				header.Infos[info.Id] = info
			}

		} else if strings.HasPrefix(line, "##PLATFORM") {
			kv := strings.Split(line, "=")
			if len(kv) == 2 {
				header.Platform = kv[1]
			}

		} else if strings.HasPrefix(line, "##FILTER") {
			filter, err := parseHeaderFilter(line)
			if err != nil {
				log.Println(err)
			}
			if len(filter) == 2 {
				header.Filters[filter[0]] = filter[1]
			}

		} else if strings.HasPrefix(line, "##contig") {
			contig, err := parseHeaderContig(line)
			if err != nil {
				log.Println(err)
			}
			if contig != nil {
				if _, ok := contig["ID"]; ok {
					header.Contigs = append(header.Contigs, contig)
				} else {
					log.Println("bad contig", contig)
				}
			}
		} else if strings.HasPrefix(line, "##SAMPLE") {
			sample, err := parseHeaderSample(line)
			if err != nil {
				log.Println(err)
			}
			if sample != "" {
				header.Samples[sample] = line
			} else {
				if err != nil {
					log.Println(err)
				}
			}
		} else if strings.HasPrefix(line, "##PEDIGREE") {
			header.Pedigrees = append(header.Pedigrees, line)
		} else if strings.HasPrefix(line, "##") {
			kv, err := parseHeaderExtraKV(line)
			if err != nil {
				log.Println(err)
			}
			if len(kv) == 2 {
				header.Extras = append(header.Extras, line)
			}

		} else if strings.HasPrefix(line, "#CHROM") {
			var err error
			header.SampleNames, err = parseSampleLine(line)
			if err != nil {
				log.Println(err)
			}
			break

		} else {
			e := fmt.Errorf("unexpected header line: %s", line)
			return nil, e
		}
	}
	return header, nil
}

// extractVcfFields parses basic information about variant
func extractVcfFields(chr string, start int, ref, alt string, info *InfoByte) (VariantInfo, error) {
	var vcf VariantInfo

	// Empty Alt
	if alt == "." || alt == "" {
		log.Println("Alt is empty", vcf, info)
		return vcf, nil
	}

	// SVtype
	var svtype string
	svtypeInterface, err := info.Get("SVTYPE")
	if err != nil && !strings.Contains(err.Error(), "not found in header") {
		log.Printf("Error getting SVType from INFO\n%v\n%v\n", info, err)
	}

	if err == nil {
		svtype = svtypeInterface.(string)
	}

	// Alt, End
	switch {
	// Deletion or Duplication
	// Duplication "1	14621	.	N	<DUP>	5.56	LOWBFSCORE	SVTYPE=DUP;END=17098;EXPECTED=175;OBSERVED=250;RATIO=1.43;BF=5.56	GT	0/1"
	// Inversion - ex. "2 321682 INV0 T <INV> 6 PASS SVTYPE=INV;END=421681"
	// LOH - "chr22	44084517	.	N	<LOH>	.	PASS	CN=2;CS=10;SVTYPE=LOH;END=50776368;NUMSNP=2405	GT	./."
	case svtype == "DEL" || svtype == "DUP" || svtype == "INV" || svtype == "LOH":
		var end int
		endInterface, err := info.Get("END")
		if err != nil && !strings.Contains(err.Error(), "not found in header") {
			log.Println("Error getting END from INFO", info, err)
		} else if endInterface != nil {
			end = endInterface.(int)
		} else {
			svLength, err := info.Get("SVLEN")
			if err != nil {
				log.Println("Error getting SVLEN from INFO", info, err)
			}

			if svLength != nil {
				end = start + svLength.(int)
			} else {
				return vcf, errors.New("structural variant without identifiable END")
			}
		}

		vcf = VariantInfo{
			VariantKey: fmt.Sprintf("%s-%d-%d-%s", strings.Replace(chr, "chr", "", 1), start+1, end, svtype),
			Chr:        chr,
			Start:      start,
			End:        end,
			Ref:        "",
			Alt:        "",
			IsSV:       true,
			SVtype:     svtype,
		}

	// ex. 2 321681 . G	G]2 : 421681] 0 PASS SVTYPE=BND;MATEID=bnd_U;EVENT=INV0
	case svtype == "BND":
		translocatedChr, translocatedStart, translocatedIsPosStrand, translocatedComesAfter := parseBreakendAlt(alt)

		vcf = VariantInfo{
			VariantKey:              fmt.Sprintf("%s-%d-%d-%s", strings.Replace(chr, "chr", "", 1), start+1, start+len(ref), svtype),
			Chr:                     chr,
			Start:                   start,
			End:                     start + 1, // Simple (wrong) assumption with little consequence right now
			Ref:                     "",
			Alt:                     "",
			IsSV:                    true,
			SVtype:                  svtype,
			TranslocatedChr:         translocatedChr,
			TranslocatedStart:       translocatedStart,
			TranslocatedIsPosStrand: translocatedIsPosStrand,
			TranslocatedComesAfter:  translocatedComesAfter,
		}

	// ex. 13 321682 INS0 T C<ctg1 > 6 PASS SVTYPE=INS
	case svtype == "INS":
		vcf = VariantInfo{
			VariantKey: fmt.Sprintf("%s-%d-%d-%s", strings.Replace(chr, "chr", "", 1), start+1, start+1, svtype),
			Chr:        chr,
			Start:      start,
			End:        start + 1,
			Ref:        ref,
			Alt:        "",
			IsSV:       true,
			SVtype:     svtype,
		}

	// NOTHING YET FOR:
	// 	"INS:ME":     InsertionMobileElement,
	// 	"CNV":        CopyNumberVariation,
	case svtype != "":
		vcf = VariantInfo{
			VariantKey: fmt.Sprintf("%s-%d-%d-%s", strings.Replace(chr, "chr", "", 1), start+1, start+len(ref), svtype),
			Chr:        chr,
			Start:      start,
			End:        start + len(ref),
			Ref:        ref,
			Alt:        "",
			IsSV:       true,
			SVtype:     svtype,
		}

	default:
		alt = strings.ToUpper(strings.Replace(alt, ".", "", -1))

		vcf = VariantInfo{
			VariantKey: fmt.Sprintf("%s-%d-%s-%s", strings.Replace(chr, "chr", "", 1), start+1, ref, alt),
			Chr:        chr,
			Start:      start,
			End:        start + len(ref),
			Ref:        ref,
			Alt:        alt,
			IsSV:       false,
			SVtype:     svtype,
		}
	}

	return vcf, nil
}

// The assertion is that ref is replaced with t, and then some piece starting at position p is joined to t. The cases are:
// ALT		Example			Meaning
// t[p[		G[2 : 421681[	piece extending to the right of p is joined after t
// t]p]		A]2 : 321681]	reverse comp piece extending left of p is joined after t
// [p[t		[2 : 321682[C	reverse comp piece extending right of p is joined before t
// ]p]t		]2 : 321681]A	piece extending to the left of p is joined before t
func parseBreakendAlt(altStr string) (joinedChr string, joinedStart int, joinedIsPosStrand, joinedIsAfter bool) {
	var sep string
	if strings.Contains(altStr, "[") {
		sep = "["
	} else {
		sep = "]"
	}

	pt := strings.Split(altStr, sep)
	t := strings.Split(pt[1], ":")
	joinedChr = strings.TrimSpace(t[0])
	posStr := strings.TrimSpace(t[1])
	joinedStart, _ = strconv.Atoi(posStr)
	joinedStart--

	switch {
	case strings.HasSuffix(altStr, "["):
		joinedIsPosStrand = true
		joinedIsAfter = true

	case strings.HasSuffix(altStr, "]"):
		joinedIsAfter = true

	case strings.HasPrefix(altStr, "["):
		joinedIsPosStrand = true

		// case strings.HasPrefix(altStr, "]"):
	}
	return
}
