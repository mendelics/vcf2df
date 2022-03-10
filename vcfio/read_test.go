package vcfio

import (
	"fmt"
	"log"
	"testing"
)

func TestReadNewVcf2(t *testing.T) {
	s, header, err := ReadNewVcf("../samples/cnv.vcf.gz")
	if err != nil {
		log.Fatalln("Failed reading VCF")
	}

	fmt.Println(header.SampleNames)
	fmt.Println(len(header.Contigs))
	for s.Scan() {
		line := s.Text()
		fmt.Println(line)
	}
	// Output:
	// [TEST-001]
	// 25
	// chr2	165959843	.	N	<DEL>	23	PASS	CN=1;SVTYPE=DEL;END=166169412;EXPECTED=741;OBSERVED=427;RATIO=0.576;BF=23	GT	0/1
	// chr7	117469210	.	N	<DEL>	23	PASS	CN=0;SVTYPE=DEL;END=117679939;EXPECTED=255;OBSERVED=0;RATIO=0.0;BF=23	GT	1/1
	// chr17	15229777	.	N	<DUP>	9.97	PASS	CN=3;SVTYPE=DUP;END=15265357;EXPECTED=67;OBSERVED=104;RATIO=1.55;BF=9.97	GT	./.
	// chrX	154021013	.	N	<DEL>	11.7	PASS	CN=1;SVTYPE=DEL;END=154101781;EXPECTED=261;OBSERVED=138;RATIO=0.529;BF=11.7	GT	0/1
}
