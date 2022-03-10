package vcfio

import (
	"log"
	"strconv"
	"strings"
)

func extractVcfMAFS(chr string, info *InfoByte) (ControlDB, string, error) {
	var g ControlInfo
	var label string

	gnomadINFO, err := info.Get("GNOMAD")
	if err == nil && gnomadINFO != nil {
		gnomadValues := strings.Split(gnomadINFO.(string), "|")

		if len(gnomadValues) != 6 {
			log.Println("Error parsing gnomad counts", err)
		}

		cnt := make([]int, 5)
		var err error
		for i := 0; i < 5; i++ {
			cnt[i], err = strconv.Atoi(gnomadValues[i])
			if err != nil {
				log.Println("Error parsing gnomad counts", err)
			}
		}

		mafMax, err := strconv.ParseFloat(gnomadValues[5], 64)
		if err != nil {
			log.Println("Error parsing gnomad counts", err)
		}

		g.AlleleCount = cnt[0]
		g.AlleleTotal = cnt[1]
		g.AlleleCountMale = cnt[2]
		g.AlleleTotalMale = cnt[3]
		g.HomoCount = cnt[4]
		g.AlleleFreqPopMax = mafMax

		if chr != "chrM" {
			g.HetCount = cnt[0] - 2*cnt[4]
		} else {
			g.HetCount = cnt[0] - cnt[4]
		}

	}

	labelStr, err := info.Get("LABEL")
	if err == nil && labelStr != nil {
		label = labelStr.([]string)[0]
	}

	return ControlDB{
		Gnomad: g,
	}, label, nil
}
