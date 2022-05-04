// Copyright 2021-2022, Matthew Winter
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/OTA-Insight/bqwriter"
	"github.com/rs/zerolog"
	"google.golang.org/api/googleapi"
)

var logger zerolog.Logger
var applicationText = "%s 0.2.2%s"
var copyrightText = "Copyright 2021-2022, Matthew Winter\n"
var indent = "..."

var helpText = `
A command line application designed to provide a method to test the BigQuery
Streaming API or BigQuery Storage Write API, allowing you to get a view of
the potential throughput available via a given host.

Use --help for more details.


USAGE:
    bqwrite-test -p PROJECT_ID -d DATASET -t TABLENAME -w WORKERS

ARGS:
`

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, applicationText, filepath.Base(os.Args[0]), "\n")
		fmt.Fprint(os.Stderr, copyrightText)
		fmt.Fprint(os.Stderr, helpText)
		flag.PrintDefaults()
	}

	// Define the Long CLI flag names
	var targetProject = flag.String("p", "", "Google Cloud Project ID  (Required)")
	var targetDataset = flag.String("d", "", "BigQuery Dataset  (Required)")
	var targetTable = flag.String("t", "bqwrite_test", "BigQuery Table")
	var numberWorkers = flag.Int("w", 5, "Number of Parallel Workers, 1 to 100")
	var numberIterations = flag.Int("i", 100, "Number of Records, 1 to 100000000")
	var batchSize = flag.Int("b", 1, "Batch Size, 1 to 50000")
	var overwriteTable = flag.Bool("o", false, "Overwrite BigQuery Table")
	var verbose = flag.Bool("v", false, "Output Verbose Detail")

	// Parse the flags
	flag.Parse()

	// Validate the Required Flags
	if *targetDataset == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Verify Number of Parallel Workers is between 1 and 100
	if *numberWorkers < 1 || *numberWorkers > 100 {
		flag.Usage()
		os.Exit(1)
	}

	// Verify Number of Iterations is between 1 and 100000000
	if *numberIterations < 1 || *numberIterations > 100000000 {
		flag.Usage()
		os.Exit(1)
	}

	// Verify Batch Size is between 1 and 50000
	if *batchSize < 1 || *batchSize > 50000 {
		flag.Usage()
		os.Exit(1)
	}

	// Setup Zero Log for Consolo Output
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	logger = zerolog.New(output).With().Timestamp().Logger()
	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Output Header
	logger.Info().Msgf(applicationText, filepath.Base(os.Args[0]), "")
	logger.Info().Msg("Arguments")
	logger.Info().Str("Project ID", *targetProject).Msg(indent)
	logger.Info().Str("Dataset", *targetDataset).Msg(indent)
	logger.Info().Str("Table", *targetTable).Msg(indent)
	logger.Info().Int("Number Workers", *numberWorkers).Msg(indent)
	logger.Info().Int("Number Records", *numberIterations).Msg(indent)
	logger.Info().Int("Batch Size", *batchSize).Msg(indent)
	logger.Info().Msg("Begin")

	// Create a BigQuery Client
	logger.Info().Msg("Establish BigQuery Client Connection")
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *targetProject)
	if err != nil {
		logger.Error().Err(err).Msg("Error [bigquery.NewClient]")
		os.Exit(1)
	}
	defer client.Close()

	// Create the Target BigQuery Table if Required
	err = CreateBigQueryTable(ctx, client, *targetDataset, *targetTable, *overwriteTable)
	if err != nil {
		logger.Error().Err(err).Msg("Error [CreateBigQueryTable]")
		os.Exit(1)
	}

	// Execute Legacy Stream to Target BigQuery Table
	err = ExecuteLegacyStream(ctx, *targetProject, *targetDataset, *targetTable, *numberWorkers, *batchSize, *numberIterations, *verbose)
	if err != nil {
		logger.Error().Err(err).Msg("Error [ExecuteLegacyStream]")
		os.Exit(1)
	}

	logger.Info().Msg("End")
}

// CreateBigQueryTable will create the target BigQuery table if required
func CreateBigQueryTable(ctx context.Context, client *bigquery.Client, datasetID, tableID string, overwrite bool) error {
	var createTable bool = false

	// Check to see if the Table Exists, if it does, delete the table
	table := client.Dataset(datasetID).Table(tableID)
	tableMetaData, err := table.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
			createTable = true
		} else {
			return err
		}
	}

	// If the table already exists and the overwrite flag is present
	if overwrite && tableMetaData != nil {
		logger.Info().Str("Table Name", tableID).Msg("Deleting Existing BigQuery Table")
		err = table.Delete(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("Error [table.Delete]")
			os.Exit(1)
		}

		// Need to add a short sleep here, for the eventual consistency issue
		logger.Info().Msg("  Sleeping for 10 minutes to allow for eventual consistency to propagate")
		time.Sleep(10 * time.Minute)

		table = client.Dataset(datasetID).Table(tableID)
		createTable = true
	}

	// Finally, Create the BigQuery Table if required
	if createTable {
		logger.Info().Str("Table Name", tableID).Msg("Creating BigQuery Table")
		if err := table.Create(ctx, &bigquery.TableMetadata{Schema: tableDataBigQuerySchema}); err != nil {
			return err
		}

		// Need to add a short sleep here, for the eventual consistency issue
		logger.Info().Msg("  Sleeping for 10 minutes to allow for eventual consistency to propagate")
		time.Sleep(10 * time.Minute)
	}

	return nil
}

// ExecuteLegacyStream will establish a stream to the target BigQuery table using the legacy API
func ExecuteLegacyStream(ctx context.Context, projectID, datasetID, tableID string, numberWorkers, batchSize, numberIterations int, verbose bool) error {
	// Create a BigQuery (stream) writer thread-safe client,
	logger.Info().Msg("Establish BigQuery Streaming Client")
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     numberWorkers,
			WorkerQueueSize: CalculateWorkerQueueSize(batchSize),
			InsertAllClient: &bqwriter.InsertAllClientConfig{
				BatchSize:            batchSize,
				FailOnInvalidRows:    true,
				FailForUnknownValues: true,
			},
		},
	)
	if err != nil {
		return err
	}
	defer streamer.Close()

	// You can now start writing data to your BQ table
	startTime := time.Now()
	count := 0
	logger.Info().Msg("Start Streaming Data")
	for data := range newGenerator(ctx, numberIterations, NewTableData) {
		err = streamer.Write(data)
		if err != nil {
			return err
		}
		count++

		if verbose {
			if math.Mod(float64(count), 10000) == 0 {
				logger.Info().Int("Records Sent", count).Msg(indent)
			}
		}
	}
	elapsed := time.Since(startTime)
	logger.Info().Int("Records Sent", count).Dur("Time Taken", elapsed).Msg(indent)
	logger.Info().Msg("End Streaming Data")
	logger.Info().Msg("Closing BigQuery Streaming Client")

	return nil
}

// CalculateWorkerQueueSize attempts to dynamically adjust the work queue size
// to minimise any records from being dropped.
func CalculateWorkerQueueSize(batchSize int) int {
	if batchSize >= 500 {
		return 100
	} else if batchSize >= 200 {
		return 50
	} else if batchSize >= 50 {
		return 10
	}
	return 1
}
