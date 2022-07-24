package main

import (
	"log"
	"os"

	"github.com/mendelics/vcf2df/converter"
	"github.com/urfave/cli/v2"
)

func main() {

	app := &cli.App{
		Name:  "vcf2df",
		Usage: "Convert VCF to parquet file",
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
				},
				Action: func(c *cli.Context) error {
					converter.Convert2parquet(c.String("vcf"), c.String("out"))
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
