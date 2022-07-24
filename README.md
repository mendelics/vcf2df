### vcf2df

Reads sample.vcf.gz and writes sample.parquet

#### Reserved column names:

- VARIANTKEY
- CHROM
- POS
- REF
- ALT
- QUAL
- FILTER
- IS_SV
- SVTYPE
- END
- NUMALTS
- SAMPLE
- IS_PHASED
- PHASE_ID

#### Samples:

If vcf has 1+ sample genotypes, the parquet file will contain 1 line per sample with > 0 alleles. If the vcf does not contain samples, all variants will be represented in the parquet file.

