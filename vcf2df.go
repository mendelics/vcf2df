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
					&cli.StringFlag{
						Name:  "info",
						Usage: "INFO value (ex. BETA",
					},
					&cli.StringFlag{
						Name:  "type",
						Value: "string",
						Usage: "INFO type (ex. string, float, int)",
					},
					&cli.StringFlag{
						Name:  "out",
						Value: "./",
						Usage: "Output folder.",
					},
				},
				Action: func(c *cli.Context) error {
					if c.String("info") != "" {
						ConvertINFO(c.String("vcf"), c.String("out"), c.String("info"), c.String("type"))
					}
					ConvertNumAlts(c.String("vcf"), c.String("out"))
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
