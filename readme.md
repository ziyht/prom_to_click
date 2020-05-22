## prom_to_click

   a remote storage for prometheus to write samples to clickhouse
   
   note: 
   * [prom2click](https://github.com/iyacontrol/prom2click) 
   * [PromHouse](https://github.com/Percona-Lab/PromHouse)
  
   prom2click and PromHouse are all not usable in my build time, 

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

the dbname and tablename can be set in prom_to_click.yml for each clickhouse_servers  
you can also set them from remote_read and remote_write in prometheus like that(higher priority):
```yaml
- url: http://localhost:9302/write?db=prometheus&table=prom_qos
  remote_timeout: 30s
  queue_config:
    max_samples_per_send: 500
remote_read:
- url: http://localhost:9302/read?db=prometheus&table=prom_qos
  remote_timeout: 20m
```

### mode1 and mode2 (**Not Recommend**)
In mode1 and mode2, we using one table to store whole data, the core process is rebuild from [prom2click](https://github.com/iyacontrol/prom2click).  
mode1 is totally the same as prom2click.  
mode2 is created because there has some bugs in read in mode1
* mode1 take more memery in clickhouse-server for 'group by' and 'sort by' operations
* mode2 take more network transfer, in this mode, only do 'sort by' operation in clickhouse-server 

for the store data is the same, so you can change mode in 1 and 2 in any time

```mysql
CREATE DATABASE IF NOT EXISTS <dbname>;
CREATE TABLE IF NOT EXISTS <dbname>.<tablename>
    (
          date Date DEFAULT toDate(0),
          name String,
          tags Array(String),
          val Float64,
          ts DateTime,
          updated DateTime DEFAULT now()
    )
    ENGINE = GraphiteMergeTree('graphite_rollup')
           PARTITION BY toYYYYMM(date)
           ORDER BY (name, tags, ts)
           SETTINGS index_granularity=8192
    );
```

### mode3 (**Recommend**)
in mode3 we'll create two tables to store data like [PromHouse](https://github.com/Percona-Lab/PromHouse)  
of course, we do some changed:  
1. unlike PromHouse, we store lables as arr, not json string
2. we do not cache the whole fingerprints in mem and update them in every 5 seconds, it cost a lot, and in our situation, the num of fingerprints will keep growth in the whole project life time
3. we do some optimization for fingerprints store and query
    1. the fingerprint will be write to clickhouse for everyday for it's first discovery, so you can delete fingerprints by day which is not needed for timeout (delete fingerprints in the same time as samples timeout and need to be deleted).
    2. for a query, first we query the fingerprints Limited by date set in the query, and then we query the samples needed.

why we recommend this mode:
1. the uncompressed data(source data) stroed in clickhouse is far less than mode1 and mode2 (only 1/5), so it will take less memory for clickhouse do 'order by' and 'sort by' operations for query
2. it'll take less time when using fingerprint(unit64) do 'order by' and 'sort by' operations 
3. less transfer data to/from clickhouse


```mysql
CREATE DATABASE IF NOT EXISTS <dbname>;

CREATE TABLE IF NOT EXISTS <dbname>.<tablename>_metrics (
			date        Date      DEFAULT toDate(now()),
            name        String,
            tags        Array(String),
			fingerprint UInt64
		)
		ENGINE = ReplacingMergeTree
			PARTITION BY toYYYYMM(date)
			ORDER BY (date, name, tags, fingerprint);

CREATE TABLE IF NOT EXISTS <dbname>.<tablename>_samples (
			fingerprint  UInt64,
			ts           DateTime,
			val          Float64
		)
		ENGINE = MergeTree
			PARTITION BY toYYYYMM(ts)
			ORDER BY (fingerprint, ts);
```

## run
> **you need to set config file first**

```shell script
./prom_to_click
```

## prometheus
using /write and /read URI for prometheus to write and read
```yaml
- url: http://localhost:9302/write
  remote_timeout: 30s
  queue_config:
    max_samples_per_send: 500
remote_read:
- url: http://localhost:9302/read
  remote_timeout: 20m
```

## 

## todo
* support config settings in cmdline
* ...