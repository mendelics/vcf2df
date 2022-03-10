package vcfio

import (
	"encoding/csv"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var typeRe = `String|Integer|Float|Flag|Character|Unknown`
var infoRegexp = regexp.MustCompile(fmt.Sprintf(`##INFO=<ID=(.+),Number=([\dAGR\.]*),Type=(%s),Description="(.*)">`, typeRe))
var formatRegexp = regexp.MustCompile(fmt.Sprintf(`##FORMAT=<ID=(.+),Number=([\dAGR\.]*),Type=(%s),Description="(.*)">`, typeRe))
var filterRegexp = regexp.MustCompile(`##FILTER=<ID=(.+),Description="(.*)">`)
var sampleRegexp = regexp.MustCompile(`SAMPLE=<ID=([^,>]+)`)
var fileVersionRegexp = regexp.MustCompile(`##fileformat=VCFv(.+)`)

// InfoVcf holds the Info and Format fields
type InfoVcf struct {
	Id          string
	Description string
	Number      string // A G R . ''
	Type        string // STRING INTEGER FLOAT FLAG CHARACTER UNKONWN
}

// SampleFormat holds the type info for Format fields.
type SampleFormat InfoVcf

// Header holds all the type and format information for the variants.
type Header struct {
	sync.RWMutex

	SampleNames   []string
	Infos         map[string]*InfoVcf
	SampleFormats map[string]*SampleFormat
	Filters       map[string]string
	Extras        []string
	FileFormat    string
	// Contigs is a list of maps of length, URL, etc.
	Contigs []map[string]string
	// ##SAMPLE
	Samples   map[string]string
	Pedigrees []string
	Platform  string
}

// String returns a string representation.
func (i *InfoVcf) String() string {
	return fmt.Sprintf("##INFO=<ID=%s,Number=%s,Type=%s,Description=\"%s\">", i.Id, i.Number, i.Type, i.Description)
}

// String returns a string representation.
func (i *SampleFormat) String() string {
	return fmt.Sprintf("##FORMAT=<ID=%s,Number=%s,Type=%s,Description=\"%s\">", i.Id, i.Number, i.Type, i.Description)
}

// NewHeader returns a Header with the requisite allocations.
func NewHeader() *Header {
	var h Header
	h.Filters = make(map[string]string)
	h.Infos = make(map[string]*InfoVcf)
	h.SampleFormats = make(map[string]*SampleFormat)
	h.SampleNames = make([]string, 0)
	h.Pedigrees = make([]string, 0)
	h.Samples = make(map[string]string)
	h.Extras = make([]string, 0)
	h.Contigs = make([]map[string]string, 0, 64)
	return &h
}

func parseHeaderInfo(info string) (*InfoVcf, error) {
	res := infoRegexp.FindStringSubmatch(info)
	if len(res) != 5 {
		return nil, fmt.Errorf("INFO error: %s, %v", info, res)
	}
	var i InfoVcf
	i.Id = res[1]
	i.Number = res[2]
	i.Type = res[3]
	i.Description = res[4]
	return &i, nil
}

func parseHeaderContig(contig string) (map[string]string, error) {
	vmap := make(map[string]string)
	contig = strings.TrimSuffix(strings.TrimPrefix(contig, "##contig=<"), ">")
	rdr := csv.NewReader(strings.NewReader(contig))
	rdr.LazyQuotes = true
	rdr.TrimLeadingSpace = true
	contigs, err := rdr.Read()

	for _, pair := range contigs {
		kv := strings.SplitN(pair, "=", 2)
		vmap[kv[0]] = kv[1]
	}
	return vmap, err
}

func parseHeaderExtraKV(kv string) ([]string, error) {
	// This is repeated bc go-staticheck alert didn't like '##'
	kv = strings.TrimLeft(kv, "#")
	kv = strings.TrimLeft(kv, "#") // Keep second repeated line
	kv = strings.TrimLeft(kv, " ")
	kvpair := strings.SplitN(kv, "=", 2)

	if len(kvpair) != 2 {
		return nil, fmt.Errorf("header error in extra field: %s", kv)
	}
	return kvpair, nil
}

func parseHeaderFormat(info string) (*SampleFormat, error) {
	res := formatRegexp.FindStringSubmatch(info)
	if len(res) != 5 {
		return nil, fmt.Errorf("FORMAT error: %s", info)
	}
	var i SampleFormat
	i.Id = res[1]
	i.Number = res[2]
	i.Type = res[3]
	i.Description = res[4]
	return &i, nil
}

func parseHeaderFilter(info string) ([]string, error) {
	res := filterRegexp.FindStringSubmatch(info)
	if len(res) != 3 {
		return nil, fmt.Errorf("FILTER error: %s", info)
	}
	return res[1:3], nil
}

// return just the sample id.
func parseHeaderSample(line string) (string, error) {
	res := sampleRegexp.FindStringSubmatch(line)
	if len(res) != 2 {
		return "", fmt.Errorf("error parsing ##SAMPLE")
	}
	return res[1], nil
}

func parseHeaderFileVersion(format string) (string, error) {
	res := fileVersionRegexp.FindStringSubmatch(format)
	if len(res) != 2 {
		return "-1", fmt.Errorf("file format error: %s", format)
	}

	return res[1], nil
}

func parseSampleLine(line string) ([]string, error) {
	fields := strings.Split(line, "\t")
	var samples []string
	if len(fields) > 9 {
		samples = fields[9:]
	} else {
		samples = []string{}
	}
	return samples, nil
}

func parseOne(key, val, itype string) (interface{}, error) {
	var v interface{}
	var err error
	switch itype {
	case "Integer", "INTEGER":
		v, err = strconv.Atoi(val)
	case "Float", "FLOAT":
		v, err = strconv.ParseFloat(val, 32)
	case "Flag", "FLAG":
		if val != "" {
			err = fmt.Errorf("Info Error: flag field (%s) had value", key)
		}
		v = true
	default:
		v = val
	}
	return v, err
}
