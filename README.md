### vcf2df

Reads sample.vcf.gz and writes sample.parquet

#### Reserved column names:

- **VARIANTKEY** (STRING)

    Variantkey is CHR-POS-REF-ALT for small variants and CHR-POS-END-SVTYPE for structural variants. CHR without chr preffix.

- **CHROM** (STRING)

    Chromosome (including chr preffix).

- **POS** (INT)

    Position (1-based)

- **REF** (STRING)

    Reference allele. Empty for structural variants.

- **ALT** (STRING)

    Alternate allele. Empty for structural variants.

- **QUAL** (INT)

    Quality score (Integer)

- **PASS** (BOOL)

    Boolean describing filter == PASS || filter == "."

- **IS_SV** (BOOL)

    Boolean structural variant.",

- **SVTYPE** (STRING)

    Structural variant type (ex. DEL, DUP, INV, ...)

- **END** (INT)

    End position of variant (1-based).

- **NUMALTS** (INT)

    Number of alternate alleles (0, 1, 2)

- **SAMPLE** (STRING)

    Sample string.

- **IS_PHASED** (BOOL)

    Boolean if variant is phased.

- **PHASE_ID** (STRING)

    String identifying variant phase.

- **REF_READS** (INT)

    Read depth for ref.

- **ALT_READS** (INT)

    Read depth for alt.

#### Samples:

If vcf has 1+ sample genotypes, the parquet file will contain 1 line per sample with > 0 alleles. If the vcf does not contain samples, all variants will be represented in the parquet file.

#### Footer:

All columns are described in the parquet metadata (footer) key-values.

