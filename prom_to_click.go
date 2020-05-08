package main

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {

	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

}
