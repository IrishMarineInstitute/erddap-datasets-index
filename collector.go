package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
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
}

func readCSVFromUrl(url string) ([][]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if(resp.StatusCode != 200) {
		return [][]string{},nil
	}
	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	//reader.Comma = ';'
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func encode(url string) (string) {
    url = strings.Replace(url,`"`,"%22",-1)
    url = strings.Replace(url,",","%2C",-1)
    url = strings.Replace(url,"(","%28",-1)
    url = strings.Replace(url,")","%29",-1)
    url = strings.Replace(url,">","%3E",-1)
    url = strings.Replace(url,"<","%3C",-1)
	return url;	
}

func listDatasets(erddap string)(chan Dataset, error){
    url := erddap + 
    		`/tabledap/allDatasets.jsonlKVP?` +
			`datasetID,dataStructure,cdm_data_type,minTime,maxTime,tabledap,metadata` +
    		`&accessible="public"` +
    		`&dataStructure="table"` +
    		`&datasetID!="allDatasets"&datasetID!="datasetsIndex"`
    url = encode(url)
    fmt.Println(url)
	resp, err := http.Get(url)
	if err != nil{
		return nil,err
	}
    datasets := make(chan Dataset, 128)


    go func(){
		defer resp.Body.Close()
		decoder := json.NewDecoder(resp.Body);
		for decoder.More() {
			var dataset Dataset
		    if err := decoder.Decode(&dataset); err != nil {
			    fmt.Println(err)
			    break;
		    }
		    dataset.Metadata,err = readCSVFromUrl(dataset.MetadataURL+".csv")
		    if err != nil{
		    	fmt.Println(err)
		    } else {
		    	for _, row := range dataset.Metadata {
		    		if row[2] == "ioos_category" && row[4] == "Identifier" && dataset.Identifier == ""{
		    			dataset.Identifier = row[1]
		    		}else if row[0] == "variable" {
		    			switch row[1] {
		    				case "latitude":
		    					dataset.Latitude = true
		    				case "longitude":
		    					dataset.Longitude = true
		    				case "time":
		    					dataset.Time = true
		    				default:
		    					break
		    			}
		    		}
		    	}
		    }

			datasets <- dataset
		}
		close(datasets)
	}()
	return datasets, nil
}

/**
 * More time buckets, each one is smaller
 */
func smaller(input []time.Time) ([]time.Time) {
	if len(input)<2 {
		return input
	}
	if input[1].Sub(input[0]).Hours() < 48 {
		return input
	}
	output := []time.Time{input[0]}
	for i :=1; i<len(input); i++ {
		output = append(output, input[i-1].Add(input[i].Sub(input[i-1])/2), input[i])
	}
	return output
}

/*
 * fewer time buckets, each one is larger
 */
func larger(input[] time.Time) ([]time.Time) {
	if len(input)<2 {
		return input
	}
	if input[1].Sub(input[0]).Hours() > 24 * 270 {
		return input
	}
	output := []time.Time{input[0]}
	for i :=2; i<len(input)-1; i+=2 {
		output = append(output, input[i])
	}
	output = append(output, input[len(input)-1])
	return output
}

func appendToFile(path string, data [][]string, dataset Dataset) (error){
	for i := range data {
		data[i] = append(data[i], dataset.DatasetID)
	}
    f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
		 return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.WriteAll(data)
	if err = w.Error(); err != nil {
		return err
	}
	w.Flush()
	return nil
}

func loadLastTimestamps(path string) (timestamps map[string]string, years map[string]bool, err error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, nil, err
    }
    defer f.Close()
    r := csv.NewReader(f)
    timestamps = make(map[string]string)
    years = make(map[string]bool)
    for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if(len(record) != 5){
			return nil, nil, errors.New("Input file not in the expected format")
		}
		timestamps[record[4]] = record[0]
		years[record[0][0:4]] = false
	}
	return timestamps, years, nil
}

/**
 * collects the data from erddap into the named file.
 * Returns a map of (string) year and whether data for that
 * year was updated during the collection
 */
func collect(erddap string, fname string) (map[string]bool, error){
	timestamps, years, err := loadLastTimestamps(fname)
	if err != nil {
		return nil, err
	}

	datasets, err := listDatasets(erddap)
	if err != nil {
		return nil, err
	}
	for dataset := range datasets {
		if dataset.TabledapURL == "" {
			log.Printf("Skipping %s which does not have an TabledapURL", dataset.DatasetID)
			continue;
		}
		if !(dataset.Latitude && dataset.Longitude && dataset.Time){
			log.Printf("Skipping %s which does not have all of latitude, longitude, time", dataset.DatasetID)
			continue;
		}
		if dataset.Identifier == "" {
			log.Printf("Skipping %s which does not have an identifier", dataset.DatasetID)
			continue;
		}
		if timestamps[dataset.DatasetID] > dataset.MinTime {
			dataset.MinTime = timestamps[dataset.DatasetID]
		}
		if dataset.MinTime == "" {
			var minTime = "2000-01-01T00:00:00Z"
			log.Printf("Warning %s has no MinTime setting to %s", dataset.DatasetID, minTime)
			dataset.MinTime = minTime
		}
		if dataset.MaxTime == "" {
			var maxTime = ( time.Now().Add(28 * 24 * time.Hour).Format("2006-01-02") ) +"T00:00:00Z"
			log.Printf("Warning %s has no MaxTime setting to %s", dataset.DatasetID, maxTime)
			dataset.MaxTime = maxTime
		}
		var startTime, endTime time.Time
		var e error
		if startTime, e = time.Parse(time.RFC3339, dataset.MinTime); e != nil {
			log.Printf("Skipping %s due to invalid MinTime %s", dataset.DatasetID, dataset.MinTime)
			continue
		}
		if endTime, e = time.Parse(time.RFC3339, dataset.MaxTime); e != nil {
			log.Printf("Skipping %s due to invalid MaxTime %s", dataset.DatasetID, dataset.MaxTime)
			continue
		}
		dataset.TimeBuckets = []time.Time{startTime,endTime}

		for dataset.TimeBuckets[1].Sub(dataset.TimeBuckets[0]).Hours() > 14 * 24 {
			dataset.TimeBuckets = smaller(dataset.TimeBuckets)
		}
		for len(dataset.TimeBuckets)>1 {
			var url = dataset.TabledapURL +
			".csv0?time," +
			dataset.Identifier +
			",latitude,longitude" +
			"&time>" +
			dataset.TimeBuckets[0].UTC().Format(time.RFC3339) +
			"&time<=" +
			dataset.TimeBuckets[1].UTC().Format(time.RFC3339) +
			`&orderByClosest("` +
				dataset.Identifier +
				`,time,1hour")`;
			url = encode(url)
			log.Printf("fetching %s",url)
			startTime = time.Now()
			if result, err := readCSVFromUrl(url); err != nil {
				log.Printf("Error: problem fetching %s",url)
				log.Print(err)
				break;
			}else{
				duration := time.Now().Sub(startTime).Seconds()
				//fmt.Println(duration)
				if len(result) == 0 && duration>30 {
					dataset.TimeBuckets = smaller(dataset.TimeBuckets)
					continue
				}
				dataset.TimeBuckets = dataset.TimeBuckets[1:]
				if duration > 10 {
					dataset.TimeBuckets = smaller(dataset.TimeBuckets)
				}else if duration < 3 {
					dataset.TimeBuckets = larger(dataset.TimeBuckets)
				}
				if len(result) > 0{
					if err := appendToFile(fname,result,dataset); err != nil {
						return nil, err
					}
					for i := range result {
						years[result[i][0][0:4]] = true
					}
				}
			}
		}

	}
	return years, nil
}

