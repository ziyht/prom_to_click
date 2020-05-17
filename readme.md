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

### mode1 and mode2

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

### mode3
in mode3 we'll create two tables to store data like promhouse

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
you need to set config file first

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