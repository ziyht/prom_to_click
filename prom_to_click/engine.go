package prom_to_click

import (
	"github.com/prometheus/prometheus/storage/remote"
	"go.uber.org/zap"
	"net/http"
)

type ptcReader interface {
	init()
	HandlePromReadReq(req *remote.ReadRequest, r *http.Request) (*remote.ReadResponse, error)
	IsHealthy() bool
}

type ptcWriter interface {
	init()
	HandlePromWriteReq(req *remote.WriteRequest, r *http.Request) (*remote.ReadResponse, error)
	IsHealthy() bool
	Start()
	Stop()
	Wait()
}

type ptcEngine struct {
	reader ptcReader
	writer ptcWriter
	server *ptcServer
	clicks *clicksMan
	log    *zap.SugaredLogger
}

var Engine *ptcEngine

func (e *ptcEngine)GetLog() *zap.SugaredLogger{
	return e.log
}

func (e *ptcEngine)StartServer(){
	e.server.Start()
}

func (e *ptcEngine)WaitServer(){
	e.server.Wait()
}

func init(){

	initConfig()
	initLogger()

	Engine = new(ptcEngine)
	Engine.log    = slog

	Engine.clicks = new(clicksMan)
	Engine.reader = new(clickReader)
	Engine.writer = new(clickWriter)
	Engine.server = new(ptcServer)

	if Cfg.Reader.Mode == 2{
		Engine.reader = new(clickReader2)
	}

	Engine.clicks.init()
	Engine.reader.init()
	Engine.writer.init()
	Engine.server.init()
}