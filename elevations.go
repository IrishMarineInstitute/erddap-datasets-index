package main

import (
	"errors"
	"fmt"
  "github.com/batchatco/go-native-netcdf/netcdf/api"
  "github.com/batchatco/go-native-netcdf/netcdf/cdf"
  "github.com/batchatco/go-native-netcdf/netcdf/hdf5"
  "github.com/golang/groupcache/lru"
  "log"
  "math"
  "sort"
)

type ElevationGetter struct {                                                           
  vg        api.VarGetter                                                       
  chunkSize int                                                                 
  slice     [][]int16                                                           
  begin     int                                                                 
  end       int                                                                 
}                                                                                  
    
// chunkSize must evenly divide the slice length                                                                            
func newElevationGetter(vg api.VarGetter, chunkSize int) *ElevationGetter {                                                                          
  return &ElevationGetter{vg, chunkSize, nil, 0, 0}                              
}                                                                               
                                                                         
func (w *ElevationGetter) Get(ilat int, ilon int) int16 {                               
  if w.slice == nil || !(ilat >= w.begin && ilat < w.end) {                     
    round := w.chunkSize * (ilat / w.chunkSize)                                 
    var err error                                                               
    var sl interface{}                                                          
    sl, err = w.vg.GetSlice(int64(round), int64(round+w.chunkSize))             
    if err != nil {                                                             
      panic(err)                                                                
    }                                                                           
    w.slice = sl.([][]int16)                                                    
    w.begin = round                                                             
    w.end = round + w.chunkSize                                                 
  }                                                                             
  return w.slice[ilat-int(w.begin)][ilon]                                       
}   


// uses experimental feature https://github.com/batchatco/go-native-netcdf/issues/2
type Elevations struct{
	lat []float64
	lon []float64
	getter *ElevationGetter
	cache *lru.Cache
	nc api.Group
}

func iclosest(x float64, data []float64) int{
	if x <= data[0]{
		return 0;
	}
	if x >= data[len(data)-1] {
		return len(data) - 1
	}

	i := sort.Search(len(data), func(i int) bool { return data[i] >= x })
	if i < len(data) && data[i] == x {
		return i
	}

	if math.Abs(x - data[i-1]) < math.Abs(data[i] - x){
		return i - 1
	}
	return i
}

func (e Elevations) Close(){
	e.nc.Close()
}

func (e Elevations) GetElevation(lat, lon float64) (int16, error){
	key1 := fmt.Sprintf("%v,%v",lat,lon)
	cached, ok := e.cache.Get(key1)
	if ok {
		return cached.(int16), nil
	}
	ilat := iclosest(lat, e.lat)
	ilon := iclosest(lon, e.lon)
	key2 := fmt.Sprintf("%v,%v",e.lat[ilat],e.lon[ilon])
	cached, ok = e.cache.Get(key2)
	if ok {
		return cached.(int16), nil
	}
	elevation := e.getter.Get(ilat,ilon)
	e.cache.Add(key1, elevation)
	e.cache.Add(key2, elevation)
	return elevation, nil
}

func OpenElevationsFile(path string) (*Elevations, error) {
	nc, err := hdf5.Open(path)
    if err != nil {
    	nc, err = cdf.Open(path)
    	if err != nil {
	        log.Println("ERROR: could not read elevations file "+path)
	        return nil, err
    	}
    }
    vr, _ := nc.GetVariable("lat")
    if vr == nil {
          return nil, errors.New("lat variable not found in elevations file "+path)
    }
    lat, has := vr.Values.([]float64)
    if !has {
          return nil, errors.New("lat data not found in elevations file "+path)
    }
    vr, _ = nc.GetVariable("lon")
    if vr == nil {
          return nil, errors.New("lon variable not found in elevations file "+path)
    }
    lon, has := vr.Values.([]float64)
    if !has {
          return nil, errors.New("lon data not found in elevations file "+path)
    }
    vg, err := nc.GetVarGetter("elevation")                                           
    if err != nil {
    	return nil, errors.New("elevation variable not found in elevations file "+path)
    }
	getter := newElevationGetter(vg, 1)

	return &Elevations{
		lat: lat,
		lon: lon,
		nc: nc,
		getter: getter,
		cache: lru.New(100000),
	}, nil

}