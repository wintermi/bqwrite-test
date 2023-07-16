# BigQuery Streaming API Test Client

[![Workflows](https://github.com/wintermi/bqwrite-test/workflows/Go/badge.svg)](https://github.com/wintermi/bqwrite-test/actions/workflows/go.yml)
[![Go Report](https://goreportcard.com/badge/github.com/wintermi/bqwrite-test)](https://goreportcard.com/report/github.com/wintermi/bqwrite-test)
[![License](https://img.shields.io/github/license/wintermi/bqwrite-test.svg)](https://github.com/wintermi/bqwrite-test/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/wintermi/bqwrite-test?include_prereleases)](https://github.com/wintermi/bqwrite-test/releases)


## Description

A command line application designed to provide a method to test the BigQuery Streaming API or BigQuery Storage Write API, allowing you to get a view of the potential throughput available via a given host.

```
USAGE:
    bqwrite-test -p PROJECT_ID -d DATASET -t TABLENAME -w WORKERS

ARGS:
  -b int
    	Batch Size, 1 to 50000 (default 1)
  -d string
    	BigQuery Dataset  (Required)
  -i int
    	Number of Records, 1 to 100000000 (default 100)
  -o	Overwrite BigQuery Table
  -p string
    	Google Cloud Project ID  (Required)
  -t string
    	BigQuery Table (default "bqwrite_test")
  -v	Output Verbose Detail
  -w int
    	Number of Parallel Workers, 1 to 100 (default 5)
```

## BigQuery Table

When you first execute the command line application it will verify if the target table exists, if not found then the table will be created.

If you wish to delete and recreate the existing table you can execute the command with the `-o` overwrite flag.

## Known Limitations

Because BigQuery's Streaming API is designed for high insertion rates, modifications to the underlying table metadata exhibit are eventually consistent when interacting with the streaming system.

Because of this, when overwriting or creating the table initially a 10 minute sleep is performed.


## License

**bqwrite-test** is released under the [Apache License 2.0](https://github.com/wintermi/bqwrite-test/blob/main/LICENSE) unless explicitly mentioned in the file header.
