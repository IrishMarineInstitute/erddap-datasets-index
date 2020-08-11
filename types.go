package main

import (
	"fmt"
	"strconv"
  	"time"
)
/*
{"datasetID":"ais_met_hydro", "accessible":"public", "dataStructure":"table", "cdm_data_type":"Point", 
"title":"AIS Met Hydro", "minTime":null, "maxTime":null, "tabledap":"https://erddap.marine.ie/erddap/tabledap/ais_met_hydro"}
*/

type Dataset struct {
	DatasetID  	string  `json:"datasetID"`
	TabledapURL  	string  `json:"tabledap"`
	MetadataURL 	string 	`json:"metadata"`
	MinTime    	string  `json:"minTime"`
	MaxTime    	string 	`json:"maxTime"`
	Metadata [][]string
	Identifier  string
	Latitude    bool
	Longitude   bool
	Time        bool
	TimeBuckets []time.Time
	InProgress  bool
}

type IndexRecord struct {
	Timestamp int64
	DatasetID string
	Identifier string
	Latitude float32
	Longitude float32
	Year int32
	Month time.Month
}

func NewIndexRecord(record []string, datasetID string) (IndexRecord) {
	if(len(record)!=4){
		panic(fmt.Sprintf("wrong number of items in record, expected 4 got %d", len(record)))
	}
	var p IndexRecord
	var err error
	var timestamp time.Time
	var _float float64
	timestamp, err = time.Parse(time.RFC3339, record[0])
	p.Timestamp = timestamp.Unix()
	p.Identifier = record[1]
        if p.Identifier == ""  {
           p.Identifier = "unknown"
        }
	if _float, err = strconv.ParseFloat(record[2], 32); err != nil {
		panic(err)
	}
	p.Latitude = float32(_float)
	if _float, err = strconv.ParseFloat(record[3], 32); err != nil {
		panic(err)
	}
	p.Longitude = float32(_float)
	p.DatasetID = datasetID
	p.Year = int32(timestamp.Year())
	p.Month = timestamp.Month()
	return p
}
