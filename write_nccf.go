package main

import (
	"encoding/csv"
	"errors"
  "github.com/batchatco/go-native-netcdf/netcdf/api"
  "github.com/batchatco/go-native-netcdf/netcdf/cdf"
  "github.com/batchatco/go-native-netcdf/netcdf/util"
  "sort"
  "strconv"
  "time"
  "os"
  "io"
  "log"
)

type IndexRecord struct {
	Timestamp int64
	DatasetID string
	Identifier string
	Latitude float32
	Longitude float32
	Year int32
	Month time.Month
}

func NewIndexRecord(record []string) (IndexRecord) {
	var p IndexRecord
	var err error
	var timestamp time.Time
	var _float float64
	timestamp, err = time.Parse(time.RFC3339, record[0])
	p.Timestamp = timestamp.Unix()
	p.Identifier = record[1]
	if _float, err = strconv.ParseFloat(record[2], 32); err != nil {
		panic(err)
	}
	p.Latitude = float32(_float)
	if _float, err = strconv.ParseFloat(record[3], 32); err != nil {
		panic(err)
	}
	p.Longitude = float32(_float)
	p.DatasetID = record[4]
	p.Year = int32(timestamp.Year())
	p.Month = timestamp.Month()
	return p
}

func loadIndexRecordsFromCsv(path string) ([]IndexRecord, error) {
	records := []IndexRecord{}
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    log.Println(len(records))
    defer f.Close()
    r := csv.NewReader(f)
    for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if(len(record) != 5){
			return nil, errors.New("Input file not in the expected format")
		}
		records = append(records, NewIndexRecord(record))
	}
	sort.Slice(records, func(i, j int) bool {
  		return records[i].DatasetID < records[j].DatasetID
	})
	return records, nil
}

func csv2nccf(prefix string, csvfname string, ncfname string){
	records, err := loadIndexRecordsFromCsv(csvfname)
	if err != nil {
		panic(err)
	}
	//log.Println(records)
    cw, err := cdf.OpenWriter(ncfname)
    if err != nil {
        panic(err)
    }
    today := time.Now().Local().Format("2006-01-02")

	var lat_min, lat_max, lon_min, lon_max = float32(0),float32(0),float32(0),float32(0)
	{ // latitude
		var min = float32(9999)
		var max = float32(-9999)
	    data := make([]float32, len(records))
	    for i := 0; i < len(records); i++ {
	  		data[i] = records[i].Latitude
	  		if data[i] < min{
	  			min = data[i]
	  		}
	  		if data[i] > max {
	  			max = data[i]
	  		}
	  	}
	    attributes, err := util.NewOrderedMap(
	  	    []string{"_CoordinateAxisType", 
	  	    		"actual_range",
	  	    		"axis",
	  	    		"colorBarMaximum",
	  	    		"colorBarMinimum",
	  	    		"ioos_category",
	  	    		"long_name",
	  	    		"sdn_p01_label",
	  	    		"sdn_p01_url",
	  	    		"sdn_p01_urn",
	  	    		"sdn_p06_urn",
	  	    		"standard_name",
	  	    		"units"},
        	map[string]interface{}{
				"_CoordinateAxisType": "Lat" ,
				"actual_range": []float32{min, max} ,
				"axis": "Y" ,
				"colorBarMaximum": float32(90) ,
				"colorBarMinimum": float32(-90) ,
				"ioos_category": "Location" ,
				"long_name": "Latitude" ,
				"sdn_p01_label": "Latitude north" ,
				"sdn_p01_url": "https://vocab.nerc.ac.uk/collection/P01/current/ALATZZ01/" ,
				"sdn_p01_urn": "SDN:P01::ALATZZ01" ,
				"sdn_p06_urn": "SDN:P06::UAAA" ,
				"standard_name": "latitude" ,
				"units": "degrees_north"})
	    if err != nil {
	        panic(err)
	    }

	    err = cw.AddVar("latitude", api.Variable{
	        data,
	        []string{"row"},
	        attributes})
	    if err != nil {
	        panic(err)
	    }
	    lat_min = min
	    lat_max = max
	}
	{ // longitude
		var min = float32(9999)
		var max = float32(-9999)
	    data := make([]float32, len(records))
	    for i := 0; i < len(records); i++ {
	  		data[i] = records[i].Longitude
	  		if data[i] < min{
	  			min = data[i]
	  		}
	  		if data[i] > max {
	  			max = data[i]
	  		}
	  	}
	    attributes, err := util.NewOrderedMap(
	  	    []string{"_CoordinateAxisType", 
	  	    		"actual_range",
	  	    		"axis",
	  	    		"BODC_Parameter_Usage_Vocabulary",
	  	    		"colorBarMaximum",
	  	    		"colorBarMinimum",
	  	    		"ioos_category",
	  	    		"long_name",
	  	    		"sdn_p01_label",
	  	    		"sdn_p01_url",
	  	    		"sdn_p01_urn",
	  	    		"sdn_p06_urn",
	  	    		"standard_name",
	  	    		"units"},
        	map[string]interface{}{
				"_CoordinateAxisType": "Lon" ,
				"actual_range": []float32{min,max},
				"axis": "X" ,
				"BODC_Parameter_Usage_Vocabulary": "Longitude east" ,
				"colorBarMaximum": float32(180) ,
				"colorBarMinimum": float32(-180) ,
				"ioos_category": "Location" ,
				"long_name": "Longitude" ,
				"sdn_p01_label": "Longitude east" ,
				"sdn_p01_url": "https://vocab.nerc.ac.uk/collection/P01/current/ALONZZ01/" ,
				"sdn_p01_urn": "SDN:P01::ALONZZ01" ,
				"sdn_p06_urn": "SDN:P06::UAAA" ,
				"standard_name": "longitude" ,
				"units": "degrees_east"})
	    if err != nil {
	        panic(err)
	    }

	    err = cw.AddVar("longitude", api.Variable{
	        data,
	        []string{"row"},
	        attributes})
	    if err != nil {
	        panic(err)
	    }
	    lon_min = min
	    lon_max = max
	}

	{ // time
		var min = float64(999999999999999999)
		var max = float64(0)
	    data := make([]float64, len(records))
	    for i := 0; i < len(records); i++ {
	  		data[i] = float64(records[i].Timestamp)
	  		if data[i] < min{
	  			min = data[i]
	  		}
	  		if data[i] > max {
	  			max = data[i]
	  		}
	  	}
	    attributes, err := util.NewOrderedMap(
	  	    []string{"_CoordinateAxisType",
					"actual_range",
					"axis",
					"ioos_category",
					"long_name",
					"sdn_p01_label",
					"sdn_p01_url",
					"sdn_p01_urn",
					"sdn_p06_urn",
					"standard_name",
					"time_origin",
					"units"},
        	map[string]interface{}{
				"_CoordinateAxisType": "Time" ,
				"actual_range": []float64{min,max},
				"axis": "T" ,
				"ioos_category": "Time" ,
				"long_name": "Time Interval" ,
				"sdn_p01_label": "Date (time from 00:00 01/01/1760 to 00:00 UT on day)" ,
				"sdn_p01_url": "https://vocab.nerc.ac.uk/collection/P01/current/AADYAA01/" ,
				"sdn_p01_urn": "SDN:P01::AADYAA01" ,
				"sdn_p06_urn": "SDN:P06::UTAA" ,
				"standard_name": "time" ,
				"time_origin": "01-JAN-1970 00:00:00" ,
				"units": "seconds since 1970-01-01T00:00:00Z"})
	    if err != nil {
	        panic(err)
	    }

	    err = cw.AddVar("time", api.Variable{
	        data,
	        []string{"row"},
	        attributes})
	    if err != nil {
	        panic(err)
	    }
	}
	{ // year
		var min = int32(9999)
		var max = int32(-9999)
	    data := make([]int32, len(records))
	    for i := 0; i < len(records); i++ {
	  		data[i] = records[i].Year
	  		if data[i] < min{
	  			min = data[i]
	  		}
	  		if data[i] > max {
	  			max = data[i]
	  		}
	  	}
	    attributes, err := util.NewOrderedMap(
	        []string{"actual_range", "coordinates","ioos_category","long_name"},
	        map[string]interface{}{
	        	"actual_range": []int32{min, max},
	        	"coordinates": "time latitude longitude",
	        	"ioos_category": "Time",
	        	"long_name": "Year"})
	    if err != nil {
	        panic(err)
	    }
	    err = cw.AddVar("year", api.Variable{
	        data,
	        []string{"row"},
	        attributes})
	    if err != nil {
	        panic(err)
	    }
	 }
	 {
	    data := make([]string, len(records))
	    for i := 0; i < len(records); i++ {
	  		data[i] = records[i].DatasetID
	  	}
	    attributes, err := util.NewOrderedMap(
	        []string{"_Encoding",
					"comment",
					"coordinates",
					"ioos_category",
					"long_name"},
	        map[string]interface{}{
			 	"_Encoding": "ISO-8859-1" ,
				"comment": "The id of ERDDAP dataset to which the row belongs" ,
				"coordinates": "time latitude longitude" ,
				"ioos_category": "Identifier" ,
				"long_name": "Dataset ID"})
	    if err != nil {
	        panic(err)
	    }

     	err = cw.AddVar("dataset_id", api.Variable{
		 		Values:     data,
		 		Dimensions: []string{"row"},
		 		Attributes: attributes})
	     if err != nil {
	         panic(err)
	     }
	 }
	 {
	    data := make([]string, len(records))
	    for i := 0; i < len(records); i++ {
	  		data[i] = records[i].Identifier
	  	}
	    attributes, err := util.NewOrderedMap(
	        []string{"_Encoding",
					"comment",
					"coordinates",
					"ioos_category",
					"long_name"},
	        map[string]interface{}{
			 	"_Encoding": "ISO-8859-1" ,
				"comment": "The code used to identify the device/platform/station in ERDDAP" ,
				"coordinates": "time latitude longitude" ,
				"ioos_category": "Identifier" ,
				"long_name": "Identifier"})
	    if err != nil {
	        panic(err)
	    }

     	err = cw.AddVar("identifier", api.Variable{
		 		Values:     data,
		 		Dimensions: []string{"row"},
		 		Attributes: attributes})
	     if err != nil {
	         panic(err)
	     }
	 }

	{
		attributes, err := util.NewOrderedMap([]string{"cdm_data_type",
				"Conventions",
				"creator_email",
				"creator_name",
				"creator_url",
				"date_created",
				"date_issued",
				"date_modified",
				"Easternmost_Easting",
				"featureType",
				"geospatial_lat_max",
				"geospatial_lat_min",
				"geospatial_lat_units",
				"geospatial_lon_max",
				"geospatial_lon_min",
				"geospatial_lon_units",
				"id",
				"infoUrl",
				"institution",
				"keywords",
				"keywords_vocabulary",
				"license",
				"licenseUrl",
				"Northernmost_Northing",
				"publisher_email",
				"publisher_name",
				"publisher_url",
				"sdn_edmo_code",
				"sourceUrl",
				"Southernmost_Northing",
				"standard_name_vocabulary",
				"subsetVariables",
				"summary",
				"time_coverage_end",
				"time_coverage_start",
				"title",
				"Westernmost_Easting"},
			map[string]interface{}{"cdm_data_type": "Point" ,
				"Conventions": "COARDS, CF-1.6, ACDD-1.3, SeaDataNet" ,
				"creator_email": "datarequests@marine.ie" ,
				"creator_name": "Marine Institute" ,
				"creator_url": "https://www.marine.ie" ,
				"date_created": today,
				"date_issued": today ,
				"date_modified": today ,
				"Easternmost_Easting": lon_max ,
				"featureType": "Point" ,
				"geospatial_lat_max": lat_max ,
				"geospatial_lat_min": lat_min ,
				"geospatial_lat_units": "degrees_north" ,
				"geospatial_lon_max": lon_max ,
				"geospatial_lon_min": lon_min ,
				"geospatial_lon_units": "degrees_east" ,
				"id": prefix ,
				"infoUrl": "https://www.marine.ie/" ,
				"institution": "Irish Marine Institute" ,
				"keywords": "air, air_temp, altimetry, atm, atm_pressure, atmosphere, atmospheric, calc, cond, cond_sbe21, currents, data, depth, depth_surface, direction, earth, Earth Science > Atmosphere > Atmospheric Winds > Surface Winds, ext, fluoresence, heading, heave, humidity, identifier, institute, interval, laboratory, latitude, longitude, marine, meteorology, optical, optical properties, pitch, pressure, properties, rel, rel_humidity, rel_wind_dirn, rel_wind_speed, roll, sal, sal_sbe21, satellite, sbe21, science, speed, surface, survey, surveyid, temp_calc_ext_sbe21, temp_sbe21, temp_ti20, temperature, ti20, time, true, true_heading, true_wind_dirn, true_wind_speed, underway, vessel, vessel_name, wind, wind_from_direction, wind_speed, winds" ,
				"keywords_vocabulary": "GCMD Science Keywords" ,
				"license": "Creative Commons Attribution 4.0 (https://creativecommons.org/licenses/by/4.0/)" ,
				"licenseUrl": "https://creativecommons.org/licenses/by/4.0/legalcode" ,
				"Northernmost_Northing": lat_max ,
				"publisher_email": "datarequests@marine.ie" ,
				"publisher_name": "Marine Institute" ,
				"publisher_url": "http://www.marine.ie" ,
				"sdn_edmo_code": "396" ,
				"sourceUrl": "(source database)" ,
				"Southernmost_Northing": lon_min ,
				"standard_name_vocabulary": "CF Standard Name Table v29" ,
				"subsetVariables": "dataset_id,identifier,year" ,
				"summary": "Index for dataset discovery" ,
				"time_coverage_end": time.Unix(records[len(records)-1].Timestamp, 0).UTC().Format(time.RFC3339),
				"time_coverage_start": time.Unix(records[0].Timestamp, 0).UTC().Format(time.RFC3339) ,
				"title": "Geospatial Index of ERDDAP Dataset Records" ,
				"Westernmost_Easting": lon_min })
				if err != nil {
					panic(err)
				}
			err = cw.AddGlobalAttrs(attributes)
			if err != nil {
				panic(err)
			}
	}
    // Close will write out the data and close the file
    err = cw.Close()
    if err != nil {
        panic(err)
    }
}
