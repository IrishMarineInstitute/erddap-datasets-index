package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)


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
			dataset.InProgress = false
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

func collect(dataset Dataset, records []IndexRecord) (bool, []IndexRecord, error){
	var startTime, endTime time.Time
	var e error
	data_fetched := false;
	collect_start_time := time.Now()
	if !dataset.InProgress {	
		if dataset.TabledapURL == "" {
			log.Printf("Skipping %s which does not have an TabledapURL", dataset.DatasetID)
			return false, records, nil
		}
		if !(dataset.Latitude && dataset.Longitude && dataset.Time){
			log.Printf("Skipping %s which does not have all of latitude, longitude, time", dataset.DatasetID)
			return false, records, nil
		}
		if dataset.Identifier == "" {
			log.Printf("Skipping %s which does not have an identifier", dataset.DatasetID)
			return false, records, nil;
		}
		if len(records) > 0 {
			collected_time := time.Unix(records[len(records)-1].Timestamp,0).UTC().Format(time.RFC3339) 
			if collected_time > dataset.MinTime {
				dataset.MinTime = collected_time
			}
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
		if startTime, e = time.Parse(time.RFC3339, dataset.MinTime); e != nil {
			log.Printf("Skipping %s due to invalid MinTime %s", dataset.DatasetID, dataset.MinTime)
			return false, records, nil
		}
		if endTime, e = time.Parse(time.RFC3339, dataset.MaxTime); e != nil {
			log.Printf("Skipping %s due to invalid MaxTime %s", dataset.DatasetID, dataset.MaxTime)
			return false, records, nil
		}
		if startTime.Unix() >= endTime.Unix() {
			log.Printf("Skipping %s which looks up to date", dataset.DatasetID)
			return false, records, nil
		}
		dataset.TimeBuckets = []time.Time{startTime,endTime}

		for dataset.TimeBuckets[1].Sub(dataset.TimeBuckets[0]).Hours() > 14 * 24 {
			dataset.TimeBuckets = smaller(dataset.TimeBuckets)
		}
		dataset.InProgress = true
	}

	for len(dataset.TimeBuckets)>1 {
		query_min_time := dataset.TimeBuckets[0].UTC().Format(time.RFC3339)
		query_max_time := dataset.TimeBuckets[1].UTC().Format(time.RFC3339)
		var url = dataset.TabledapURL +
		".csv0?time," +
		dataset.Identifier +
		",latitude,longitude" +
		"&time>" +
		query_min_time + 
		"&time<=" +
		query_max_time +
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
			if duration > 40 {
				dataset.TimeBuckets = smaller(dataset.TimeBuckets)
			}else if duration < 20 {
				dataset.TimeBuckets = larger(dataset.TimeBuckets)
			}
			for i := range result {
				if result[i][0] > query_min_time && result[i][0] <= query_max_time{ //  Some datasets return > as >= ...
					records = append(records,NewIndexRecord(result[i],dataset.DatasetID))
					data_fetched = true
				}
			}
			duration = time.Now().Sub(collect_start_time).Seconds()
			if duration > 120 { // write data to disk so not to lose too much work
				log.Println(duration)
				return data_fetched, records, nil
			}
		}
	}

	return data_fetched, records, nil
}

