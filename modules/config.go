package modules

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
	Host     	   string   `yaml:"host"`
	User     	   string   `yaml:"user"`
	Passwd   	   string   `yaml:"passwd"`
	Database 	   string   `yaml:"database"`
	Table    	   string   `yaml:"table"`
	ReadTimeout    int      `yaml:"read_timeout"`
	WriteTimeout   int      `yaml:"write_timeout"`
	AltHosts     []string   `yaml:"alt_hosts"`
}

type ReaderCfg struct {
	MaxSamples   int      `yaml:"max_samples"`
	MinStep      int      `yaml:"min_step"`
	Quantile     float32  `yaml:"quantile"`
	Clickhouse   string   `yaml:"clickhouse"`
	Mode         int      `yaml:"mode"`
	Utc          bool     `yaml:"utc"`
}

type WriterCfg struct {
	Clickhouse   string   `yaml:"clickhouse"`
	Batch        int      `yaml:"batch"`
	Buffer       int      `yaml:"buffer"`
	Wait         int      `yaml:"wait"`
}

type LoggerCfg struct{
	Dir          	string `yaml:"dir"`
	MaxSize     	int    `yaml:"max_size"`
	MaxBackups  	int    `yaml:"max_backups"`
	MaxAge      	int    `yaml:"max_age"`
	Compress    	bool   `yaml:"compress"`
	LevelConsole    string `yaml:"level_console"`
	LevelFile       string `yaml:"level_file"`
}

type ptcCfg struct{
	Server  ServerCfg
	Servers map[string]ClickCfg  `yaml:"clickhouse_servers"`
	Reader  ReaderCfg
	Writer  WriterCfg
	Logger  LoggerCfg
}

var Cfg ptcCfg

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


