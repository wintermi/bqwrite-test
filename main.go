// Copyright 2021, Matthew Winter
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
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/OTA-Insight/bqwriter"
	"github.com/google/logger"
	"google.golang.org/api/googleapi"
)

var applicationText = "%s 0.1.0\n"
var copyrightText = "Copyright 2021, Matthew Winter\n"

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
		fmt.Fprintf(os.Stderr, applicationText, filepath.Base(os.Args[0]))
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

	// Setup the standard Google Logger
	defer logger.Init("bqwrite-test", *verbose, true, os.Stdout).Close()
	logger.SetFlags(log.LstdFlags)
	logger.SetFlags(log.Lmicroseconds)
	logger.Infof(applicationText, filepath.Base(os.Args[0]))
	logger.Info()
	logger.Info("Parameters")
	logger.Infof("  Project ID:     %v", *targetProject)
	logger.Infof("  Dataset:        %v", *targetDataset)
	logger.Infof("  Table:          %v", *targetTable)
	logger.Infof("  Number Workers: %v", *numberWorkers)
	logger.Infof("  Number Records: %v", *numberIterations)
	logger.Infof("  Batch Size:     %v", *batchSize)
	logger.Info()

	// Create a BigQuery Client
	logger.Info("Establish BigQuery Client Connection")
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *targetProject)
	if err != nil {
		logger.Errorf("Error [bigquery.NewClient]: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Create the Target BigQuery Table if Required
	err = CreateBigQueryTable(ctx, client, *targetDataset, *targetTable, *overwriteTable)
	if err != nil {
		logger.Errorf("Error [CreateBigQueryTable]: %v\n", err)
		os.Exit(1)
	}

	// Create a BigQuery (stream) writer thread-safe client,
	logger.Info("Establish BigQuery Streaming Client")
	bqWriter, err := bqwriter.NewStreamer(
		ctx, *targetProject, *targetDataset, *targetTable,
		&bqwriter.StreamerConfig{
			WorkerCount:     *numberWorkers,
			WorkerQueueSize: *batchSize,
			//MaxBatchDelay:   1,
			InsertAllClient: &bqwriter.InsertAllClientConfig{
				BatchSize:            *batchSize,
				FailOnInvalidRows:    true,
				FailForUnknownValues: true,
			},
		},
	)
	if err != nil {
		logger.Errorf("Error [bqwriter.NewStreamer]: %v\n", err)
		os.Exit(1)
	}
	defer bqWriter.Close()

	// You can now start writing data to your BQ table
	startTime := time.Now()
	count := 0
	logger.Info("Start Streaming Data")
	for data := range newGenerator(ctx, *numberIterations, NewTableData) {
		err = bqWriter.Write(data)
		if err != nil {
			logger.Errorf("Error [bqwriter.Write]:\nData: %v\nError: %w\n\n", data, err)
			os.Exit(1)
		}
		count++

		if *verbose {
			if math.Mod(float64(count), 2000) == 0 {
				logger.Infof("  Records Sent: %8d", count)
			}
		}
	}
	elapsed := time.Since(startTime)
	logger.Infof("  %d Records Streamed in %s", count, elapsed)
	logger.Info("End Streaming Data")
}

// Create the Target BigQuery Table if Required
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
		logger.Infof("Deleting Existing BigQuery Table: %s", tableID)
		table.Delete(ctx)

		// Need to add a short sleep here, for the eventual consistency issue
		logger.Info("  Sleeping for 10 minutes to allow for eventual consistency to propagate")
		time.Sleep(10 * time.Minute)

		table = client.Dataset(datasetID).Table(tableID)
		createTable = true
	}

	// Finally, Create the BigQuery Table if required
	if createTable {
		logger.Infof("Creating BigQuery Table: %s", tableID)
		if err := table.Create(ctx, &bigquery.TableMetadata{Schema: tableDataBigQuerySchema}); err != nil {
			return err
		}

		// Need to add a short sleep here, for the eventual consistency issue
		logger.Info("  Sleeping for 10 minutes to allow for eventual consistency to propagate")
		time.Sleep(10 * time.Minute)
	}

	return nil
}
