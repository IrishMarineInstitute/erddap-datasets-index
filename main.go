package main

import (
	"path/filepath"
	"fmt"
	"log"
	"os"
	)

func usage(problem string){
	if problem != "" {
		fmt.Fprintf(os.Stderr, `ERROR: %s
`, problem)
	}
	fmt.Fprintf(os.Stderr, `USAGE: %s --erddap <erddap> --elevations <file> --archive <folder> [--flag flag]
		erddap is the erddap url eg: https://erddap.marine.ie/erddap
		elevations is path to the lat,lon,elevation netcdf file (eg: the 7.5G unzipped GEBCO elevations grid netcdf file from https://www.gebco.net/data_and_products/gridded_bathymetry_data/ )
		archive is the folder to contain the index eg erddap-marine-ie-index
		flag is the /path/to/flag/datasetsIndex flag file
`, os.Args[0])
	os.Exit(2)
}

func main(){
	if len(os.Args) < 7 {
		usage("")
	}
	var erddap, archive, elevations_path, flag string
	for i:=1; i < len(os.Args); i++ {
		switch p := os.Args[i]; p {
			case "--erddap":
				i++
				if i == len(os.Args){
					usage("missing option for --erddap")
				}
				erddap = os.Args[i]
			case "--elevations":
				i++
				if i == len(os.Args){
					usage("missing option for --elevations")
				}
				elevations_path = os.Args[i]
			case "--archive":
				i++
				if i == len(os.Args){
					usage("missing option for --archive")
				}
				archive = os.Args[i]
			case "--flag":
				i++
				if i == len(os.Args){
					usage("missing option for --flag")
				}
				flag = os.Args[i]
			case "--help":
				usage("")
			default:
				usage("unrecognised option "+os.Args[i])
		}
	}
	if erddap == "" || archive == "" || elevations_path == ""{
		usage("")
	}
	
	elevations, err := OpenElevationsFile(elevations_path)
	if err != nil {
		log.Fatal(err)
	}
	defer elevations.Close()

	_ = os.MkdirAll(archive, 0755)
	datasets, err := listDatasets(erddap)
	if err != nil {
		log.Fatal(err)
	}
	for dataset := range datasets {
		ncfname := filepath.Join(archive, dataset.DatasetID+".nc")
		//_, err2 := os.Stat(ncfname);
		records, err := read_nccf(ncfname, elevations)
		if err != nil {
			log.Fatal(err)
		}
        if len(records)>0 {
		  write_nccf(dataset.DatasetID,ncfname,records)
        }
		touch_flag := false
		for do_continue, data_fetched, records, err := collect(dataset, records, elevations); data_fetched; do_continue, data_fetched, records, err = collect(dataset, records, elevations) {
			if err != nil {
				log.Fatal(err)
			}
			if data_fetched {
				write_nccf(dataset.DatasetID,ncfname,records)
				if flag != "" {
					touch_flag = true;
				}
			}
            if !do_continue {
                  break
            }
		}
		if err != nil {
			log.Fatal(err)
		}
		if touch_flag {
			_, err := os.Stat(flag)
		    if os.IsNotExist(err) {
		        file, err := os.Create(flag)
		        if err != nil {
		            log.Fatal(err)
		        }
		        defer file.Close()
		        log.Println("touched flag file "+flag)
		    }
		}

	}

}
