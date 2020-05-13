package main

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	modules "github.com/ziyht/prom_to_click/modules"
)

func main() {

	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	modules.Engine.StartServer()
	modules.Engine.WaitServer()
}
