package src

import "database/sql"

type Server struct {
	cfg    *ServerCfg
	db     *sql.DB
	health bool
}

var Servers map[string]*Server

func parsingServers() {

	for key, cfg range CfgRoot.Servers{
		
	}

}

func init() {

	Servers = map[string]*Server{}

	parsingServers()
}
