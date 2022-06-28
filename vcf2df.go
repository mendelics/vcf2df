package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

// GitVersion is stored at compile time
var GitVersion string

func main() {

	app := &cli.App{
		Name:  "vcf2df",
		Usage: "Convert VCF to Pandas dataframe parquet file",
		Commands: []*cli.Command{
			{
				Name:  "convert",
				Usage: "Read sample.vcf.gz and write sample.parquet",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "vcf",
						Required: true,
						Usage:    "Single vcf file (.vcf.gz)",
					},
					&cli.BoolFlag{
						Name:  "usesample",
						Usage: "Use sample name as output file name.",
					},
					&cli.BoolFlag{
						Name:  "full",
						Usage: "Output all vcf columns instead of just numalts.",
					},
					&cli.StringFlag{
						Name:  "info",
						Usage: "INFO field to be output instead of numalts.",
					},
					&cli.StringFlag{
						Name:  "out",
						Value: "./",
						Usage: "Output folder.",
					},
				},
				Action: func(c *cli.Context) error {
					// Check VCF file
					missingVcfs := checkIfVcfsExist([]string{c.String("vcf")})
					if len(missingVcfs) != 0 {
						log.Fatalf("missing vcfs: %v", missingVcfs)
					}

					outputFilename, outputPath := createOutputFile(c.String("vcf"), c.String("out"), c.Bool("usesample"))

					convert2parquet(c.String("vcf"), outputFilename, outputPath, c.Bool("usesample"), c.Bool("full"), c.String("info"))

					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
