package src

import (
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

var (
	configFile = kingpin.Flag("config.file", "the config path").Default("./prom_to_click.yml").String()
)

type ServerCfg struct {
	Addr  	       string   `yaml:"addr"`
	Timeout        int      `yaml:"timeout"`
}

type ClickCfg struct {
	Dsn            string   `yaml:"dsn"`
	Server   	   string   `yaml:"server"`
	User     	   string   `yaml:"user"`
	Passwd   	   string   `yaml:"passwd"`
	Database 	   string   `yaml:"database"`
	Table    	   string   `yaml:"table"`
	ReadTimeout    int      `yaml:"read_timeout"`
	WriteTimeout   int      `yaml:"write_timeout"`
	AltHosts     []string   `yaml:"alt_hosts"`
}

type ReaderCfg struct {
	MaxSamplers  int      `yaml:"max_samples"`
	Percentile   float32  `yaml:"percentile"`
	Clickhouse   string   `yaml:"clickhouse"`
}

type WriterCfg struct {
	Clickhouse   string   `yaml:"clickhouse"`
}

type LoggerCfg struct{
	Path        string `yaml:"path"`
	MaxSize     int    `yaml:"max_size"`
	MaxBackups  int    `yaml:"max_backups"`
	MaxAge      int    `yaml:"max_age"`
	Compress    bool   `yaml:"compress"`
}

type cfg struct{
	Server  ServerCfg
	Servers map[string]ClickCfg  `yaml:"clickhouse_servers"`
	Reader  ReaderCfg
	Writer  WriterCfg
	Logger  LoggerCfg
}

var Cfg cfg

func initConfig()  {

	kingpin.Parse()

	buffer, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("%s", err)
	}

	err = yaml.Unmarshal(buffer, &Cfg)
	if err != nil {
		log.Fatalf(err.Error())
	}
}


