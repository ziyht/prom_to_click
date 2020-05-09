package src

import (
	"go.uber.org/zap"
	"gopkg.in/tylerb/graceful.v1"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/remote"
)

type p2cServer struct {
	cfg         *ServerCfg
	requests 	chan *promSample
	mux      	*http.ServeMux
	log      	*zap.SugaredLogger
	recvCounter prometheus.Counter
}

var server *p2cServer

func NewP2CServer() *p2cServer {

	recvCounter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	prometheus.MustRegister(recvCounter)

	server := new(p2cServer)
	server.cfg          = &Cfg.Server
	server.mux 			= http.NewServeMux()
	server.log 			= slog
	server.recvCounter  = recvCounter

	server.mux.HandleFunc("/read", handlerForPathRead)
	server.mux.HandleFunc("/write", handlerForPathWrite)
	//server.mux.HandleFunc("/metrics", prometheus.InstrumentHandler(
	//	"/metrics", prometheus.UninstrumentedHandler(),
	//))

	return server
}

func handlerForPathRead(w http.ResponseWriter, r *http.Request){
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req remote.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resp *remote.ReadResponse
	resp, err = reader.HandlePromReadReq(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func handlerForPathWrite(w http.ResponseWriter, r *http.Request){

	slog.Debug("handle: %s", r.URL)

	return
}

func (c *p2cServer)StartServer() error{
	slog.Infof("HTTP server starting at %s ...", c.cfg.Addr)

	return graceful.RunWithErr(c.cfg.Addr, time.Second * time.Duration(c.cfg.Timeout), c.mux)
}

func (c *p2cServer)Shutdown() {
	close(c.requests)
	//c.writer.Wait()

	wchan := make(chan struct{})
	go func() {
		//c.writer.Wait()
		close(wchan)
	}()

	select {
	case <-wchan:
		slog.Info("Writer shutdown cleanly..")
	// All done!
	case <-time.After(10 * time.Second):
		slog.Info("Writer shutdown timed out, samples will be lost..")
	}
}

func GetServer() *p2cServer{
	return server
}

func initServer(){
	server = NewP2CServer()
}