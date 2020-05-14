## prom_to_click

   a remote storage for prometheus to write samples to clickhouse
   
   note: rebuilt from [prom2click](https://github.com/iyacontrol/prom2click) 
   
   first, you can see project PromHouse
   
## compile
```shell script
git clone ...
cd prom_to_click
go mod tidy
go mod vendor
go build prom_to_click.go
```

## tables
we will create db and table automatically

```mysql
CREATE DATABASE IF NOT EXISTS metrics;
CREATE TABLE IF NOT EXISTS metrics.samples
    (
          date Date DEFAULT toDate(0),
          name String,
          tags Array(String),
          val Float64,
          ts DateTime,
          updated DateTime DEFAULT now()
    )
    ENGINE = GraphiteMergeTree(
          date, (name, tags, ts), 8192, 'graphite_rollup'
    );
```

## run
you need to set config file first

```shell script
./prom_to_click
```



## prometheus
using /write and /read URI for prometheus to write and read


## 

## todo
* to update core logic to PromHouse, maybe more efficient