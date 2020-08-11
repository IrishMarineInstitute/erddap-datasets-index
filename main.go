package main

import (
	"path/filepath"
	"fmt"
	"log"
	"os"
	)

func usage(command string){
	fmt.Fprintf(os.Stderr, `USAGE: %s --erddap <erddap> --archive <folder>
		erddap is the erddap url eg: https://erddap.marine.ie/erddap
		folder is the folder to contain the index eg erddap-marine-ie-index
`, command)
	os.Exit(2)
}

func main(){
	if len(os.Args) != 5 || os.Args[1] != "--erddap" || os.Args[3] != "--archive" {
		usage(os.Args[0])
	}
	erddap := os.Args[2]
	path := os.Args[4]
	_ = os.MkdirAll(path, 0755)
	datasets, err := listDatasets(erddap)
	if err != nil {
		log.Fatal(err)
	}
	for dataset := range datasets {
		ncfname := filepath.Join(path, dataset.DatasetID+".nc")
		//_, err2 := os.Stat(ncfname);
		records, err := read_nccf(ncfname)
		if err != nil {
			log.Fatal(err)
		}
		nrecords := len(records)
		records, err = collect(dataset, records)
		if err != nil {
			log.Fatal(err)
		}
		if len(records) != nrecords {
			write_nccf(dataset.DatasetID,ncfname,records)
		}
	}

}