package main

import (
	"path/filepath"
	"fmt"
	"log"
	"os"
	"sync"
	"bufio"
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
	index_csv := filepath.Join(path,"index.csv")
	years, err := collect(erddap,index_csv)
	if err != nil {
		log.Fatal(err)
	}
	channels := make(map[string](chan string))
	var wg sync.WaitGroup
	for year, updated := range years {
		ncfname := filepath.Join(path, year+".nc")
		_, err2 := os.Stat(ncfname);
		if updated ||  os.IsNotExist(err2) {
			years[year] = true
			channels[year] = make(chan string, 2)
			wg.Add(1)
			go func(csvpath string, channel chan string){
				defer wg.Done()
				log.Println("writing "+csvpath)
     			f, err := os.OpenFile(csvpath, os.O_CREATE|os.O_WRONLY, 0644)
			    if err != nil {
					 log.Fatal(err)
				}
				defer f.Close()
				for line := range channel {
					if _, err = f.WriteString(line+"\n"); err != nil {
					    log.Fatal(err)
					}
				}

			}(filepath.Join(path, year+".csv"), channels[year])
		}
	}
	{
		file, err := os.Open(index_csv)
	    if err != nil {
			 log.Fatal(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
	        line := scanner.Text()
	        year := line[0:4]
	        if years[year]{
	        	channels[year] <- line
	        }
	    }
	    if err := scanner.Err(); err != nil {
	        log.Fatal(err)
	    }
	}
	for _, channel := range channels {
		close(channel)
	}
	wg.Wait()
	for year, updated := range years {
		if updated {
			ncfname := filepath.Join(path, year+".nc")
			csvfname := filepath.Join(path, year+".csv")
			log.Println("writing "+ncfname)
			csv2nccf(year,csvfname,ncfname)
			err := os.Remove(csvfname)
			if err != nil{
				log.Fatal(err)
			}
		}
	}

}