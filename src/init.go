package src

func init()  {
	initConfig()
	initLogger()
	initClickhouse()
	initReader()
	initWriter()
	initServer()
}
