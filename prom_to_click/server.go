package prom_to_click

import (
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/remote"
)

type ptcServer struct {
	cfg         *ServerCfg
	mux      	*http.ServeMux
	log      	*zap.SugaredLogger
	recvCounter prometheus.Counter
	wg          sync.WaitGroup
	tag         string
}

func (s *ptcServer)init(){
	recvCounter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	prometheus.MustRegister(recvCounter)

	s.tag          = "server"
	s.cfg          = &Cfg.Server
	s.mux 		   = http.NewServeMux()
	s.log 		   = slog
	s.recvCounter  = recvCounter

	s.mux.HandleFunc("/read", s.handlerForPathRead)
	s.mux.HandleFunc("/write", s.handlerForPathWrite)
	s.mux.Handle("/metrics", promhttp.Handler())
}

func (s *ptcServer)handlerForPathRead(w http.ResponseWriter, r *http.Request){

	slog.Debugf("%s: %s from %s @ %s", s.tag, r.RequestURI, r.Header.Get("User-Agent"), r.RemoteAddr)

	if Engine.reader.IsHealthy() == false{
		slog.Errorf("%s: %s from %s @ %s, reject because reader is not healthy", s.tag, r.RequestURI, r.Header.Get("User-Agent"), r.RemoteAddr)
		http.Error(w, "reader is not healthy", http.StatusInternalServerError)
		return
	}

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
	resp, err = Engine.reader.HandlePromReadReq(&req, r)
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

func (s *ptcServer)handlerForPathWrite(w http.ResponseWriter, r *http.Request){

	slog.Debugf("%s: %s from %s @ %s", s.tag, r.RequestURI, r.Header.Get("User-Agent"), r.RemoteAddr)

    if Engine.writer.IsHealthy() == false{
    	slog.Errorf("%s: %s from %s @ %s, reject because writer is not healthy", s.tag, r.RequestURI, r.Header.Get("User-Agent"), r.RemoteAddr)
		http.Error(w, "writer is not healthy", http.StatusInternalServerError)
		return
	}

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

	var req remote.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	Engine.writer.HandlePromWriteReq(&req, r)

	return
}

func (s *ptcServer)Start(){
	slog.Infof("HTTP server starting at %s ...", s.cfg.Addr)

	server := &http.Server{
		Addr:         s.cfg.Addr,
		Handler:      s.mux,
	}

	s.wg.Add(1)
	go server.ListenAndServe()

	s.listenSignal(context.Background(), server)
}

func (s *ptcServer)listenSignal(ctx context.Context, httpSrv *http.Server) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-sigs:
		slog.Info("server stopping...")
		httpSrv.Shutdown(ctx)

		slog.Infof("writer stopping...")
		Engine.writer.Stop()

		waitChan := make(chan struct{})
		go func() {
			Engine.writer.Wait()
			slog.Infof("writer stopped")
			close(waitChan)
		}()

		select {
		case <-waitChan:
			slog.Info("server stopped") // All done!
		case <-time.After(10 * time.Second):
			slog.Info("writer shutdown timed out, samples will be lost..")
		}
	}

	s.wg.Done()
}

func (s *ptcServer)Wait(){
	s.wg.Wait()
}