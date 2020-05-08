


package src

type ServerCfg struct {
	Server   	 string   `yaml:"server"`
	User     	 string   `yaml:"user"`
	Passwd   	 string   `yaml:"passwd"`
	Database 	 string   `yaml:"database"`
	ReadTimeout  string   `yaml:"read_timeout"`
	WriteTimeout string   `yaml:"write_timeout"`
	AltHosts     string[] `yaml:"alt_hosts"`
}


type RootCfg struct{
	Servers map[string]ServerCfg  `yaml:"clickhouse_servers"`
}

var CfgRoot RootCfg


