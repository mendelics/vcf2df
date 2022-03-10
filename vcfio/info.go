package vcfio

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type InfoByte struct {
	InfoVcf []byte

	header *Header
}

func NewInfoByte(info []byte, h *Header) *InfoByte {
	if len(info) == 1 && info[0] == '.' {
		return &InfoByte{InfoVcf: []byte(""), header: h}
	}
	return &InfoByte{InfoVcf: info, header: h}
}

// return the start and end positions of the value.
// for flag the value is the flag.
func getpositions(info []byte, key string) (start, end int) {
	//if ipos := strings.LastIndex(bytesToString(info), ";"+key+"="); ipos != -1 {
	if ipos := bytes.Index(info, []byte(";"+key+"=")); ipos != -1 {
		start = ipos + 2 + len(key)
		for end := start + 1; end < len(info)-1; end++ {
			if info[end] == ';' {
				return start, end - 1
			}
		}
		return start, len(info) - 1
	}

	bkey := []byte(key)
	ipos, pos := 0, 0

	for {
		if pos >= len(info) {
			return -1, -1
		}
		if pos == 0 {
			ipos = bytes.Index(info, bkey)
		} else {
			ipos = bytes.Index(info[pos:], bkey)
		}

		if ipos == -1 {
			return -1, -1
		}
		pos += ipos
		if pos != 0 && info[pos-1] != ';' {
			pos += 1
			continue
		}
		eq := pos + bytes.IndexByte(info[pos:], '=')
		// at end of field and we found an Flag
		var semi int
		if eq == -1 {
			return pos, len(info)
		} else if eq-pos != len(bkey) {
			// found a longer key with same prefix.
			semi = bytes.IndexByte(info[pos:], ';')
			// flag field
			if semi == -1 {
				semi = len(info)
			} else {
				semi += pos
			}
			if semi-pos == len(bkey) {
				return pos, semi - 1
			}
			pos = semi + 1
			continue
		} else {
			semi = bytes.IndexByte(info[pos+1:], byte(';'))
		}
		if semi > -1 && eq > pos+semi {
			// should be a flag.
			return pos, pos + semi
		}

		// not at end of info field
		if semi != -1 {
			semi += pos
		}
		return eq + 1, semi
	}
}

func (i InfoByte) Contains(key string) bool {
	// short-circuit common case.
	if !bytes.Contains(i.InfoVcf, []byte(key+"=")) {
		return false
	}
	s, _ := getpositions(i.InfoVcf, key)
	return s != -1
}

func (i InfoByte) Keys() []string {
	sp := bytes.Split(i.InfoVcf, []byte{';'})
	keys := make([]string, 0, len(sp))
	for _, pair := range sp {
		key := bytes.SplitN(pair, []byte{'='}, 2)[0]
		keys = append(keys, string(key))
	}
	return keys

}

func (i *InfoByte) Delete(key string) {
	s, e := getpositions(i.InfoVcf, key)
	if s == -1 {
		return
	}
	// check if it's a flag
	if s != 0 && i.InfoVcf[s-1] != ';' {
		s -= (len(key) + 1)
	}
	if s < 0 {
		s = 0
	}
	if e == -1 {
		e = len(i.InfoVcf)
	} else {
		e += 2
	}
	if s == 0 && e == len(i.InfoVcf) {
		i.InfoVcf = i.InfoVcf[:0]
	} else if e < len(i.InfoVcf) {
		i.InfoVcf = append(i.InfoVcf[:s], i.InfoVcf[e:]...)
	} else {
		i.InfoVcf = i.InfoVcf[:s-1]
	}
}

func ItoS(k string, v interface{}) string {
	if _, ok := v.(bool); ok {
		return k
	} else if v == nil {
		return "."
	} else {
		switch v := v.(type) {
		case []interface{}:
			vs := v
			out := make([]string, len(vs))
			for i, vv := range vs {
				out[i] = ItoS(k, vv)
			}
			return strings.Join(out, ",")
		case float32:
			return fmtFloat32(v)
		case float64:
			return fmtFloat64(v)
		case int:
			return fmt.Sprintf("%d", v)
		case uint32:
			return fmt.Sprintf("%d", v)
		case []float64:
			vals := v
			svals := make([]string, len(vals))
			for i, val := range vals {
				svals[i] = fmtFloat64(float64(val))
			}
			return strings.Join(svals, ",")
		case []float32:
			vals := v
			svals := make([]string, len(vals))
			for i, val := range vals {
				svals[i] = fmtFloat32(float32(val))
			}
			return strings.Join(svals, ",")
		case []int:
			vals := v
			svals := make([]string, len(vals))
			for i, val := range vals {
				svals[i] = strconv.Itoa(int(val))
			}
			return strings.Join(svals, ",")
		case []int32:
			vals := v
			svals := make([]string, len(vals))
			for i, val := range vals {
				svals[i] = strconv.Itoa(int(int32(val)))
			}
			return strings.Join(svals, ",")
		case []string:
			return strings.Join(v, ",")

		default:
			return v.(string)
		}
	}
}

func (i InfoByte) SGet(key string) []byte {
	var sub []byte
	if key == "" || len(i.InfoVcf) == 1 {
		return sub
	}
	start, end := getpositions(i.InfoVcf, key)
	if start == -1 {
		return sub
	}
	if end == -1 {
		end = len(i.InfoVcf) - 1
	}
	val := i.InfoVcf[start : end+1]
	return val
}

// Get a value from the bytes typed according to the header.
func (i InfoByte) Get(key string) (interface{}, error) {
	v := string(i.SGet(key))
	skey := string(key)
	var ok bool
	var hi *InfoVcf
	if i.header == nil || i.header.Infos == nil {
		ok = false
	} else {
		i.header.RLock()
		hi, ok = i.header.Infos[key]
		i.header.RUnlock()
	}
	if !ok {
		err := fmt.Errorf("Info Error: %s not found in header", skey)
		// flag
		if skey == v {
			return true, err
		}
		if v == "" {
			return nil, err
		}
		return v, err
	}

	if v == "" {
		var err error
		var val interface{} = nil
		if hi.Type != "Flag" {
			err = fmt.Errorf("Info Error: %s not found in INFO", skey)
		} else {
			val = false
		}
		return val, err
	}
	if v == key {
		var err error
		if hi.Type != "Flag" {
			err = fmt.Errorf("Info Error: flag field (%s) should be specified as such in the header", skey)
		}
		return true, err
	}

	var err error
	var iv interface{}
	if hi.Number == "1" || (hi.Number == "." && strings.IndexByte(v, ',') == -1) {
		return parseOne(skey, v, hi.Type)
	}

	switch hi.Number {

	case "0":
		if hi.Type != "Flag" {
			err = fmt.Errorf("Info Error: flag field (%s) should have Number=0", skey)
		}
		return true, err

	case "R", "A", "G", "2", "3", ".":
		vals := strings.Split(v, ",")
		var vi interface{}
		switch hi.Type {
		case "Integer":
			vi = make([]int, len(vals))
			for j, val := range vals {
				iv, _ = parseOne(skey, val, hi.Type)
				vi.([]int)[j] = iv.(int)
			}
		case "Float":
			vi = make([]float32, len(vals))
			for j, val := range vals {
				iv, _ = parseOne(skey, val, hi.Type)
				if o, ok := iv.(float32); ok {
					vi.([]float32)[j] = o
				} else {
					vi.([]float32)[j] = float32(iv.(float64))
				}

			}
		case "String":
			vi = make([]string, len(vals))
			for j, val := range vals {
				iv, _ = parseOne(skey, val, hi.Type)
				vi.([]string)[j] = iv.(string)
			}
		default:
			vi = make([]interface{}, len(vals))
			for j, val := range vals {
				iv, _ = parseOne(skey, val, hi.Type)
				vi.([]interface{})[j] = iv
			}
		}
		return vi, nil

	default:
		vals := strings.Split(v, ",")
		var vi interface{} = make([]interface{}, len(vals))
		if _, err := strconv.Atoi(hi.Number); err == nil {
			for j, val := range vals {
				iv, _ = parseOne(skey, val, hi.Type)
				vi.([]interface{})[j] = iv
			}
		}
		return vi, err

	}
}

func (i InfoByte) String() string {
	if len(i.InfoVcf) == 0 {
		return "."
	}
	return string(i.InfoVcf)
}

func (i InfoByte) Bytes() []byte {
	return i.InfoVcf
}

func (i *InfoByte) UpdateHeader(key string, value interface{}) {
	i.header.Lock()
	defer i.header.Unlock()
	if i.header != nil {
		switch value := value.(type) {
		case bool:
			i.header.Infos[key] = &InfoVcf{Id: key, Description: key, Number: "0", Type: "Flag"}
		case string:
			i.header.Infos[key] = &InfoVcf{Id: key, Description: key, Number: "1", Type: "Character"}
		case int, int32, int64, uint32, uint64:
			i.header.Infos[key] = &InfoVcf{Id: key, Description: key, Number: "1", Type: "Integer"}
		case float32, float64:
			i.header.Infos[key] = &InfoVcf{Id: key, Description: key, Number: "1", Type: "Float"}
		case []interface{}:
			v := value[0]
			i.UpdateHeader(key, v)
		}
	}
}

func (i *InfoByte) Set(key string, value interface{}) error {
	if len(i.InfoVcf) == 0 {
		if v, ok := value.(bool); ok {
			if v {
				i.InfoVcf = []byte(key)
			}
		} else {
			i.InfoVcf = []byte(fmt.Sprintf("%s=%s", key, ItoS(key, value)))
		}
		return nil
	}
	s, e := getpositions(i.InfoVcf, key)
	if s == -1 || s == len(i.InfoVcf) {
		if b, ok := value.(bool); ok {
			if b {
				i.InfoVcf = append(i.InfoVcf, ';')
				i.InfoVcf = append(i.InfoVcf, key...)
			}
			return nil
		}
		slug := fmt.Sprintf(";%s=%s", key, ItoS(key, value))
		i.InfoVcf = append(i.InfoVcf, slug...)
		//i.UpdateHeader(key, value)
		return nil
	}
	if b, ok := value.(bool); ok {
		if !b {
			i.Delete(key)
		}
		return nil
	}
	slug := []byte(ItoS(key, value))
	if e == -1 {
		i.InfoVcf = append(i.InfoVcf[:s], slug...)
	} else {
		i.InfoVcf = append(i.InfoVcf[:s], append(slug, i.InfoVcf[e+1:]...)...)
	}
	return nil
}

func (i *InfoByte) Add(key string, value interface{}) {
	i.Set(key, value)
}
