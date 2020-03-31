# Wham BAM, Thank You CRAM!

## Usage

<!-- TODO -->

### `mpistat-tools.rkt`

Usage:

    mpistat-tools.rkt [MPISTATS] GROUP [SUFFIX]

Filters the `MPISTATS` file (defaults to `stdin`) by the given Unix
group and plaintext filename suffix (defaults to `.bam`). Uncompressed
output is sent to a TTY, but it will be automatically `gzip`'d if
redirected elsewhere.

### `base64-suffix.rkt`

Usage:

    base64-suffix.rkt [SUFFIX]

Experimentally determine the minimal base64 suffix triplet for the given
plaintext suffix (defaults to `.bam`). By default, 10 trials are
performed, but this can be overridden with the `TRIALS` environment
variable.
