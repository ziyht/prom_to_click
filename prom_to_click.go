package main

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	modules "github.com/ziyht/prom_to_click/src"
)

func main() {

	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	log    := modules.GetLog()
	server := modules.GetServer()

	err := server.StartServer()
	if err != nil {
		log.Fatalf("http server returned error: %s\n", err.Error())
	}

	log.Info("Shutting down..")
	server.Shutdown()
	log.Info("Exiting..")
}
