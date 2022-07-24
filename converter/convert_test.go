package converter

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	goparquet "github.com/fraugster/parquet-go"
)

// TestConvert2parquet - func defineSchemaMsg(infos map[string]*vcfio.InfoVcf) (string, []infoField, error) {
func TestConvert2parquet(t *testing.T) {
	tt := []struct {
		vcfPath       string
		parquetPath   string
		expectedCount int
	}{
		{"../samples/snv.vcf.gz", "snv.parquet", 5462},
		{"../samples/cnv.vcf.gz", "cnv.parquet", 4},
	}
	for i, test := range tt {
		// Delete Parquet leftover from failed tests
		err := deleteParquet(test.parquetPath)
		if err != nil {
			t.Errorf("Error deleting parquet file %s", test.parquetPath)
		}

		// Write Parquet
		Convert2parquet(test.vcfPath, "./")

		// Read Parquet
		count, err := readParquet(test.parquetPath)
		if err != nil {
			t.Errorf("Error reading parquet file %s", test.parquetPath)
		}

		// Tests
		if count != test.expectedCount {
			t.Errorf("Test number %d: expected count: %d, obtained count: %d\n", i, test.expectedCount, count)
		}

		// Delete Parquet
		err = deleteParquet(test.parquetPath)
		if err != nil {
			t.Errorf("Error deleting parquet file %s", test.parquetPath)
		}
	}
}

func deleteParquet(file string) error {
	// Remove old files
	if fileExists(file) {
		err := os.Remove(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func readParquet(file string) (int, error) {
	r, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		return 0, err
	}

	count := 0
	for {
		row, err := fr.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("reading record failed: %w", err)
		}

		log.Printf("Record %d:", count)
		for k, v := range row {
			if vv, ok := v.([]byte); ok {
				v = string(vv)
			}
			log.Printf("\t%s = %v", k, v)
		}

		count++
	}

	return count, nil
}
