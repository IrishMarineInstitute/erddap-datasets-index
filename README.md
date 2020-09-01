# ERDDAP datasets index

requres go version 9 or later


## installation
```
go get github.com/batchatco/go-native-netcdf/netcdf/api
go get github.com/batchatco/go-native-netcdf/netcdf/cdf
go get github.com/batchatco/go-native-netcdf/netcdf/util
go get github.com/golang/groupcache/lru
go build .

```

## running
```
mkdir -p /data
./index-datasets-erddap --erddap https://erddap.marine.ie/erddap --archive /data
touch /erddap/flag/datasetIndex
```

