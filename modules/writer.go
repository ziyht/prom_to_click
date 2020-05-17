package modules

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"net/http"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type promSample struct {
	name string
	tags []string
	val  float64
	ts   time.Time
}

type clickOutput struct {
	tag          		string
	cw                  *clickWriter
	db           		string
	table        		string
	writeCounter       	prometheus.Counter
	writeFailedCounter 	prometheus.Counter
	test     			prometheus.Counter
	timings  			prometheus.Histogram
	totalRecv			uint64
	totalWrite          uint64
	inputs   			chan *promSample
}

func NewClickOutput(cw *clickWriter, db string, table string) (out *clickOutput) {

	out = new(clickOutput)

	out.tag   = cw.tag + "->" + cw.click.tag + "/" + db + "." + table
	out.cw    = cw
	out.db    = db
	out.table = table

	out.inputs = make(chan *promSample, cw.cfg.Buffer)

	return out
}

func (co *clickOutput) HandleError(err error) error {

	compile, _ := regexp.Compile("(Database|Table) .* doesn't exist")

	if compile.Match([]byte(err.Error())) {
		return co.cw.TryCreateDatabaseTable(co)
	} else {
		return err
	}

	return nil
}

func (co *clickOutput)Stop(){
	close(co.inputs)
}

func (co *clickOutput) Start() {

	w := co.cw

	wait := w.cfg.Wait

	sigSample := new(promSample)
	// wake up wirter fo every 1 seconds
	go func() {

		for {
			time.Sleep(time.Second)
			select {
			case co.inputs <- sigSample:
			default:
			}
		}

	}()

	var insertSQL = `INSERT INTO %s.%s (date, name, tags, val, ts) VALUES (?, ?, ?, ?, ?)`

	go func() {
		w.wg.Add(1)
		slog.Infof("%s: started", co.tag)

		sql    := fmt.Sprintf(insertSQL, co.db, co.table)
		chanOK := true

		var reqs []*promSample

		for chanOK {
			w.test.Add(1)

			// get next batch of requests
			tstart := time.Now()
			for i := 0; i < w.cfg.Batch; i++ {
				var req *promSample

				// get request and also check if channel is closed
				req, chanOK = <-co.inputs

				if !chanOK {
					slog.Infof("%s: stopping...", co.tag)
					break
				}

				// we need to filter out sigSample
				if req != sigSample{
					reqs = append(reqs, req)
				}

				if wait > 0 && time.Now().Sub(tstart) > time.Second * time.Duration(wait) {
					break
				}
			}

			// ensure we have something to send..
			nmetrics := len(reqs)
			if nmetrics < 1 {
				continue
			}

			start := time.Now()

			// post them to db all at once
			tx, err := w.click.db.Begin()
			if err != nil {
				slog.Errorf("%s: begin transaction: %s", co.tag, err.Error())
				w.writeFailedCounter.Add(1.0)
				w.click.TryConnect()		// if connect failed, the health status will be set to false, and reject receive new samples
				continue
			}

			// build statements
			smt, err := tx.Prepare(sql)
			if err != nil {
				tx.Commit()

				err2 := co.HandleError(err)		// create database and table here
				if err2 != nil {
					slog.Errorf("%s: prepare statement: %s", co.tag, err.Error())
					slog.Errorf("%s: auto create table failed: %s", co.tag, err2.Error())

					continue
				} else {

					tx, err = w.click.db.Begin()
					if err != nil {
						slog.Errorf("%s: begin transaction: %s", co.tag, err.Error())
						w.writeFailedCounter.Add(1.0)
						continue
					}

					smt, err = tx.Prepare(sql)
					if err != nil {
						tx.Commit()

						slog.Errorf("%s: prepare statement: %s", co.tag, err.Error())
						continue
					}
				}
			}

			for _, req := range reqs {
				// ensure tags are inserted in the same order each time
				// possibly/probably impacts indexing?
				sort.Strings(req.tags)
				_, err = smt.Exec(req.ts, req.name, clickhouse.Array(req.tags),
					req.val, req.ts)

				if err != nil {
					slog.Errorf("%s: statement exec: %s", co.tag, err.Error())
					w.writeFailedCounter.Add(1.0)
				}
			}

			// commit and record metrics
			if err = tx.Commit(); err != nil {
				slog.Errorf("%s: commit failed: %s", co.tag, err.Error())
				w.writeFailedCounter.Add(1.0)

				w.click.TryConnect()

			} else {
				reqs = []*promSample{}				// write ok, clear reqs

				w.totalWrite += uint64(nmetrics)

				slog.Infof("%s: write %d samples, total: %d, cost: %s", co.tag, nmetrics, w.totalWrite, time.Now().Sub(start).String())

				w.writeCounter.Add(float64(nmetrics))
				w.timings.Observe(float64(time.Since(tstart)))
			}
		}

		slog.Infof("%s: stopped", co.tag)

		w.wg.Done()
	}()
}

type clickWriter struct {
	tag      			string
	cfg      			*WriterCfg
	//inputs   			chan *promSample
	wg       			sync.WaitGroup
	click    			*click
	writeCounter       	prometheus.Counter
	writeFailedCounter 	prometheus.Counter
	test     			prometheus.Counter
	timings  			prometheus.Histogram
	totalRecv			uint64
	totalWrite          uint64
	outputs             map[string]*clickOutput
}

func (w *clickWriter)init(){

	w.tag = "writer"
	w.cfg = &Cfg.Writer

	if w.cfg.Batch < 1 {
		w.cfg.Batch = 32768
	}
	if w.cfg.Buffer < 1 {
		w.cfg.Buffer = 32768
	}
	if w.cfg.Wait < 1 {
		w.cfg.Wait = -1
	}

	w.click = Engine.clicks.GetServer(w.cfg.Clickhouse)
	if w.click == nil{
		slog.Fatalf("%s: clickhouse '%s' set in writer can not be found", w.tag, w.cfg.Clickhouse)
	}

	w.writeCounter       = prometheus.NewCounter( prometheus.CounterOpts{ Name: "write_samples_total"       , Help: "Total number of processed samples sent to remote storage."})
	w.writeFailedCounter = prometheus.NewCounter( prometheus.CounterOpts{ Name: "write_failed_samples_total", Help: "Total number of processed samples which failed on send to remote storage."})
	w.test               = prometheus.NewCounter( prometheus.CounterOpts{ Name: "prometheus_remote_storage_sent_batch_duration_seconds_bucket_test", Help: "Test metric to ensure backfilled metrics are readable via prometheus.",})
	w.timings            = prometheus.NewHistogram( prometheus.HistogramOpts{Name: "write_batch_duration_seconds", Help: "Duration of sample batch send calls to the remote storage.", Buckets: prometheus.DefBuckets})
	prometheus.MustRegister(w.writeCounter)
	prometheus.MustRegister(w.writeFailedCounter)
	prometheus.MustRegister(w.test)
	prometheus.MustRegister(w.timings)

	w.outputs = map[string]*clickOutput{}
}

func (w *clickWriter) Start() {
	return // do nothing is ok
}

func (w *clickWriter) IsHealthy() bool {
	return w.click.IsHealthy()
}

func (w *clickWriter) HandleError(err error, co *clickOutput) error {

	compile, _ := regexp.Compile("(Database|Table) .* doesn't exist")

	if compile.Match([]byte(err.Error())) {
		return w.TryCreateDatabaseTable(co)
	} else {
		return err
	}

	return nil
}

func (w *clickWriter) TryCreateDatabaseTable(co *clickOutput) error{

	creatDBSql    := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", co.db)
	creatTableSql := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s
				(
					  date Date DEFAULT toDate(0),
					  name String,
					  tags Array(String),
					  val Float64,
					  ts DateTime,
					  updated DateTime DEFAULT now()
				)
				ENGINE = GraphiteMergeTree('graphite_rollup')
				   PARTITION BY toYYYYMM(date)
				   ORDER BY (name, tags, ts)
				   SETTINGS index_granularity=8192
				;`, co.db, co.table)
	{
		_, err := w.click.Exec(creatDBSql)
		if err != nil{
			return err
		}
	}

	{
		_, err := w.click.Exec(creatTableSql)
		if err != nil{
			return err
		}
	}

	return nil
}

func (w *clickWriter)getClickOutput(r *http.Request) (*clickOutput, error){
	r.ParseForm()

	dbName := w.click.cfg.Database
	tbName := w.click.cfg.Table
	{
		args, ok := r.Form["db"]
		if ok {
			dbName = args[0]
		}
	}

	{
		args, ok := r.Form["table"]
		if ok {
			tbName = args[0]
		}
	}

	if dbName == "" || tbName == "" {
		return nil, fmt.Errorf("invald dbName '%s' or tbName '%s'", dbName, tbName)
	}

	coName := dbName + "." + tbName
	co, ok := w.outputs[coName]
	if !ok {
		co = NewClickOutput(w, dbName, tbName)
		co.Start()

		w.outputs[coName] = co
	}

	return co, nil
}

func (w *clickWriter)HandlePromWriteReq(req *remote.WriteRequest, r *http.Request) (*remote.ReadResponse, error){

	curRecvs := 0

	co, err := w.getClickOutput(r)
	if err != nil{
		slog.Errorf("%s: get clickOutput failed: %s", w.tag, err)
		return nil, nil
	}

	for _, series := range req.Timeseries {
		curRecvs += len(series.Samples)

		var (
			name string
			tags []string
		)

		for _, label := range series.Labels {
			if model.LabelName(label.Name) == model.MetricNameLabel {
				name = label.Value
			}
			// store tags in <key>=<value> format
			// allows for has(tags, "key=val") searches
			// probably impossible/difficult to do regex searches on tags
			t := fmt.Sprintf("%s=%s", label.Name, label.Value)
			tags = append(tags, t)
		}

		for _, sample := range series.Samples {
			sp := new(promSample)
			sp.name = name
			sp.ts = time.Unix(sample.TimestampMs/1000, 0)
			sp.val = sample.Value
			sp.tags = tags

			co.inputs <- sp
		}
	}

	w.totalRecv  += uint64(curRecvs)
	co.totalRecv += uint64(curRecvs)

	Engine.server.recvCounter.Add(float64(curRecvs))
	slog.Infof("%s: received %d samples, total: %d", co.tag, curRecvs, co.totalRecv)

	return nil, nil
}



func (w *clickWriter) Stop() {

	for _, co := range w.outputs {
		co.Stop()
	}
}

func (w *clickWriter) Wait() {
	w.wg.Wait()
}