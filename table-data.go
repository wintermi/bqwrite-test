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
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
)

// tableDataBigQuerySchema is the BigQuery Schema for tableDataRecord
var tableDataBigQuerySchema = bigquery.Schema{
	&bigquery.FieldSchema{
		Name: "name",
		Type: bigquery.StringFieldType,
	},
	&bigquery.FieldSchema{
		Name: "uuid",
		Type: bigquery.IntegerFieldType,
	},
	&bigquery.FieldSchema{
		Name: "create_time",
		Type: bigquery.DateTimeFieldType,
	},
}

// tableDataRecord is the data structure used to hold a single records
// worth of data ready for streaming to BigQuery
type tableDataRecord struct {
	name        string
	uuid        int64
	create_time time.Time
}

// Save implements bigquery.ValueSaver.Save
func (td *tableDataRecord) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return map[string]bigquery.Value{
		"name":        td.name,
		"uuid":        td.uuid,
		"create_time": td.create_time.Format("2006-01-02 15:04:05"),
	}, bigquery.NoDedupeID, nil
}

// Save implements json.JsonMarshaler.MarshalJSON
func (td *tableDataRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":        td.name,
		"uuid":        td.uuid,
		"create_time": td.create_time,
	})
}

// Interface for Data Generation
type dataGenerator = func(name string, uuid int64, create_time time.Time) interface{}

// NewTableData creates a ValueSaver/JsonMarshal-based temporary data model, implented using the dataGenerator syntax.
func NewTableData(name string, uuid int64, create_time time.Time) interface{} {
	return &tableDataRecord{
		name:        name,
		uuid:        uuid,
		create_time: create_time,
	}
}

// Random names list used for Data Generation
var randomNames = []string{"Louis Green", "Skyla Morrison", "Annalise Rosario", "Francisco Cole", "Aron Downs", "Alvin Buck",
	"Fletcher Clarke", "Sophie Salazar", "Kaleigh Hughes", "Winston Mason", "Braelyn Ho", "Finley Gibson"}

// Generate a Random Data Set
func newGenerator(ctx context.Context, iterations int, gen dataGenerator) <-chan interface{} {
	ch := make(chan interface{}, 1)
	go func() {
		defer close(ch)
		loc, _ := time.LoadLocation("UTC")
		for i := 0; i < iterations; i++ {
			data := gen(
				randomNames[i%len(randomNames)],
				int64(i)*42,
				time.Now().In(loc),
			)

			select {
			case <-ctx.Done():
				return
			case ch <- data:
			}
		}
	}()
	return ch
}
