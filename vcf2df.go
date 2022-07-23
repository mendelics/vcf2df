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
						Name:  "out",
						Value: "./",
						Usage: "Output folder.",
					},
					&cli.BoolFlag{
						Name:  "beta",
						Usage: "Change BETA field to filename",
					},
				},
				Action: func(c *cli.Context) error {
					convert2parquet(c.String("vcf"), c.String("out"), c.Bool("beta"))
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
