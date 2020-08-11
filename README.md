# ERDDAP datasets index


## installation
```
go get github.com/batchatco/go-native-netcdf/netcdf/api
go get github.com/batchatco/go-native-netcdf/netcdf/cdf
go get github.com/batchatco/go-native-netcdf/netcdf/util
go build .

```

## running
```
mkdir -p /data
./index-erddap --erddap https://erddap.marine.ie/erddap --archive /data
touch /erddap/flag/datasetIndex
```

