### vcf2df

Reads sample.vcf.gz and writes sample.parquet

#### Reserved column names:

- **VARIANTKEY**
    Variantkey is CHR-POS-REF-ALT for small variants and CHR-POS-END-SVTYPE for structural variants. CHR without chr preffix.

- **CHROM**
    Chromosome (including chr preffix).

- **POS**
    Position (1-based)

- **REF**
    Reference allele. Empty for structural variants.

- **ALT**
    Alternate allele. Empty for structural variants.

- **QUAL**
    Quality score (Integer)

- **PASS**
    Boolean describing filter == PASS || filter == "."

- **IS_SV**
    Boolean structural variant.",

- **SVTYPE**
    Structural variant type (ex. DEL, DUP, INV, ...)

- **END**
    End position of variant (1-based).

- **NUMALTS**
    Number of alternate alleles (0, 1, 2)

- **SAMPLE**
    Sample string.

- **IS_PHASED**
    Boolean if variant is phased.

- **PHASE_ID**
    String identifying variant phase.

- **REF_READS**
    Read depth for ref.

- **ALT_READS**
    Read depth for alt.

#### Samples:

If vcf has 1+ sample genotypes, the parquet file will contain 1 line per sample with > 0 alleles. If the vcf does not contain samples, all variants will be represented in the parquet file.

##### Footer:

All columns are described in the parquet metadata (footer) key-values.

