package main

import (
  "github.com/batchatco/go-native-netcdf/netcdf/api"
  "github.com/batchatco/go-native-netcdf/netcdf/cdf"
  "github.com/batchatco/go-native-netcdf/netcdf/util"
  "sort"
  "time"
  "os"
  "log"
)


func read_nccf(ncfname string, elevations *Elevations) ([]IndexRecord, error){
    records := make([]IndexRecord,0)
    nc, err := cdf.Open(ncfname)
    if err != nil {
        log.Println("no data read from "+ncfname)
        return records, nil
    }
    defer nc.Close()
    log.Println("reading "+ncfname)
    {
        vr, _ := nc.GetVariable("latitude")
        if vr == nil {
            panic("latitude variable not found")
        }
        lats, has := vr.Values.([]float32)
        if !has {
            panic("latitude data not found")
        }
        for _, lat := range lats {
            var p IndexRecord
            p.Latitude = lat
            records = append(records,p)
        }
    }
    {
        vr, _ := nc.GetVariable("longitude")
        if vr == nil {
            panic("longitude variable not found")
        }
        lons, has := vr.Values.([]float32)
        if !has {
            panic("longitude data not found")
        }
        for i, lon := range lons {
            records[i].Longitude = lon
        }
    }
    {
        vr, _ := nc.GetVariable("elevation")
        if vr == nil && vr != nil { //TODO
            levations, has := vr.Values.([]int16)
            if !has {
                panic("elevation data not found")
            }
            for i, elevation := range levations {
                records[i].Elevation = elevation
            }
        }else{
          // Used only during migration phase, August 2020, can be deleted.
            log.Println("adding elevation variable not already in file, "+ncfname)
          for i := 0; i < len(records); i++ { 
              records[i].Elevation, err = elevations.GetElevation(float64(records[i].Latitude), float64(records[i].Longitude))
              if err != nil {
                log.Fatal(err);
              }
          }
        }
    }
    {
        vr, _ := nc.GetVariable("time")
        if vr == nil {
            panic("time variable not found")
        }
        times, has := vr.Values.([]float64)
        if !has {
            panic("time data not found")
        }
        for i, tim := range times {
            records[i].Timestamp = int64(tim)
        }
    }
    {
        vr, _ := nc.GetVariable("year")
        if vr == nil {
            panic("year variable not found")
        }
        years, has := vr.Values.([]int32)
        if !has {
            panic("year data not found")
        }
        for i, year := range years {
            records[i].Year = year
        }
    }
    {
        vr, _ := nc.GetVariable("month")
        if vr == nil {
            panic("month variable not found")
        }
        months, has := vr.Values.([]int32)
        if !has {
            panic("month data not found")
        }
        for i, month := range months {
            records[i].Month = month
        }
    }
    {
        vr, _ := nc.GetVariable("dataset_id")
        if vr == nil {
            panic("dataset_id variable not found")
        }
        dsids, has := vr.Values.([]string)
        if !has {
            panic("dataset_id data not found")
        }
        for i, dsid := range dsids {
            records[i].DatasetID = dsid
        }
    }
    {
        vr, _ := nc.GetVariable("identifier")
        if vr == nil {
            panic("identifier variable not found")
        }
        identifiers, has := vr.Values.([]string)
        if !has {
            panic("identifier data not found")
        }
        for i, identifier := range identifiers {
            records[i].Identifier = identifier
        }
    }
    sort.Slice(records, func(i, j int) bool {
        return records[i].Timestamp < records[j].Timestamp
    })

    return records, nil
}

func write_nccf(prefix string, ncfname string, records []IndexRecord){

    sort.Slice(records, func(i, j int) bool {
        return records[i].Timestamp < records[j].Timestamp
    })
    
    tmpname := ncfname+".tmp"
    os.Remove(tmpname)
    cw, err := cdf.OpenWriter(tmpname)
    if err != nil {
        panic(err)
    }
    defer os.Remove(tmpname) // cleanup if anything goes wrong
    log.Println("writing "+ncfname)
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
                      "colorBarMaximum",
                      "colorBarMinimum",
                      "ioos_category",
                      "long_name",
                      "standard_name",
                      "units"},
            map[string]interface{}{
                "_CoordinateAxisType": "Lon" ,
                "actual_range": []float32{min,max},
                "axis": "X" ,
                "colorBarMaximum": float32(180) ,
                "colorBarMinimum": float32(-180) ,
                "ioos_category": "Location" ,
                "long_name": "Longitude" ,
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
    { // elevation
        min := int16(9999)
        max := int16(-9999)
        data := make([]int16, len(records))
        for i := 0; i < len(records); i++ {
              data[i] = records[i].Elevation
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
                      "standard_name",
                      "units"},
            map[string]interface{}{
                "_CoordinateAxisType": "Height" ,
                "actual_range": []int16{min,max},
                "axis": "Z" ,
                "colorBarMaximum": int16(8000) ,
                "colorBarMinimum": int16(-8000) ,
                "ioos_category": "Location" ,
                "long_name": "  Elevation relative to sea level" ,
                "standard_name": "height_above_reference_ellipsoid" ,
                "units": "m"})
        if err != nil {
            panic(err)
        }

        err = cw.AddVar("elevation", api.Variable{
            data,
            []string{"row"},
            attributes})
        if err != nil {
            panic(err)
        }
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
                    "standard_name",
                    "time_origin",
                    "units"},
            map[string]interface{}{
                "_CoordinateAxisType": "Time" ,
                "actual_range": []float64{min,max},
                "axis": "T" ,
                "ioos_category": "Time" ,
                "long_name": "Time Interval" ,
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
    { // month
        var min = int32(9999)
        var max = int32(-9999)
        data := make([]int32, len(records))
        for i := 0; i < len(records); i++ {
              data[i] = records[i].Month
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
                "long_name": "Month"})
        if err != nil {
            panic(err)
        }
        err = cw.AddVar("month", api.Variable{
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
                "Conventions": "COARDS, CF-1.6, ACDD-1.3" ,
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
                "keywords": "latitude, longitude, elevation, time" ,
                "keywords_vocabulary": "GCMD Science Keywords" ,
                "license": "Creative Commons Attribution 4.0 (https://creativecommons.org/licenses/by/4.0/)" ,
                "licenseUrl": "https://creativecommons.org/licenses/by/4.0/legalcode" ,
                "Northernmost_Northing": lat_max ,
                "publisher_email": "datarequests@marine.ie" ,
                "publisher_name": "Marine Institute" ,
                "publisher_url": "http://www.marine.ie" ,
                "sourceUrl": "(source database)" ,
                "Southernmost_Northing": lon_min ,
                "standard_name_vocabulary": "CF Standard Name Table v29" ,
                "subsetVariables": "dataset_id,identifier,year" ,
                "summary": "Index for dataset geospatial discovery using locations of actual data rather than summarised bounding boxes from dataset metadata. Irish Marine Institute data from a local source.",
                "time_coverage_end": time.Unix(records[len(records)-1].Timestamp, 0).UTC().Format(time.RFC3339),
                "time_coverage_start": time.Unix(records[0].Timestamp, 0).UTC().Format(time.RFC3339) ,
                "title": "Geospatial Index of ERDDAP Dataset Records" ,
                "Westernmost_Easting": lon_min,
                 })
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
    os.Remove(ncfname) // if it exists
    err = os.Rename(tmpname,ncfname)
    if err != nil {
        panic(err)
    }
}
