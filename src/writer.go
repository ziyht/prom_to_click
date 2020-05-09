package src

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var insertSQL = `INSERT INTO %s.%s
	(date, name, tags, val, ts)
	VALUES	(?, ?, ?, ?, ?)`

type promSample struct {
	name string
	tags []string
	val  float64
	ts   time.Time
}

type p2cWriter struct {
	tag      string
	cfg      *WriterCfg
	inputs   chan *promSample
	wg       sync.WaitGroup
	click    *click
	tx       prometheus.Counter
	ko       prometheus.Counter
	test     prometheus.Counter
	timings  prometheus.Histogram
}

var writer *p2cWriter

func NewWrite() *p2cWriter {

	w := new(p2cWriter)

	w.tag = "writer"
	w.cfg = &Cfg.Writer

	return w
}

func (w *p2cWriter)init(){

	w.click = ClicksMan.GetServer(w.cfg.Clickhouse)
	if w.click == nil{
		slog.Fatalf("%s: clickhouse '%s' set in writer can not be found", w.tag, w.cfg.Clickhouse)
	}

	w.inputs = make(chan *promSample, w.cfg.buffer)

	w.tx      = prometheus.NewCounter( prometheus.CounterOpts{ Name: "sent_samples_total"  , Help: "Total number of processed samples sent to remote storage."})
	w.ko      = prometheus.NewCounter( prometheus.CounterOpts{ Name: "failed_samples_total", Help: "Total number of processed samples which failed on send to remote storage."})
	w.test    = prometheus.NewCounter( prometheus.CounterOpts{ Name: "prometheus_remote_storage_sent_batch_duration_seconds_bucket_test", Help: "Test metric to ensure backfilled metrics are readable via prometheus.",})
	w.timings = prometheus.NewHistogram( prometheus.HistogramOpts{Name: "sent_batch_duration_seconds", Help: "Duration of sample batch send calls to the remote storage.", Buckets: prometheus.DefBuckets})
	prometheus.MustRegister(w.tx)
	prometheus.MustRegister(w.ko)
	prometheus.MustRegister(w.test)
	prometheus.MustRegister(w.timings)
}

func (w *p2cWriter) Start() {

	go func() {
		w.wg.Add(1)
		slog.Infof("%s: started", w.tag)
		sql := fmt.Sprintf(insertSQL, w.click.cfg.Database, w.click.cfg.Table)
		ok := true
		for ok {
			w.test.Add(1)
			// get next batch of requests
			var reqs []*promSample

			tstart := time.Now()
			for i := 0; i < w.cfg.batch; i++ {
				var req *promSample
				// get requet and also check if channel is closed
				req, ok = <-w.inputs
				if !ok {
					slog.Infof("%s: stopping...", w.tag)
					break
				}
				reqs = append(reqs, req)
			}

			// ensure we have something to send..
			nmetrics := len(reqs)
			if nmetrics < 1 {
				continue
			}

			// post them to db all at once
			tx, err := w.click.db.Begin()
			if err != nil {
				slog.Infof("%s: begin transaction: %s", w.tag, err.Error())
				w.ko.Add(1.0)
				continue
			}

			// build statements
			smt, err := tx.Prepare(sql)
			for _, req := range reqs {
				if err != nil {
					slog.Infof("%s: prepare statement: %s", w.tag, err.Error())
					w.ko.Add(1.0)
					continue
				}

				// ensure tags are inserted in the same order each time
				// possibly/probably impacts indexing?
				sort.Strings(req.tags)
				_, err = smt.Exec(req.ts, req.name, clickhouse.Array(req.tags),
					req.val, req.ts)

				if err != nil {
					slog.Infof("%s: statement exec: %s", w.tag, err.Error())
					w.ko.Add(1.0)
				}
			}

			// commit and record metrics
			if err = tx.Commit(); err != nil {
				slog.Infof("%s: commit failed: %s", w.tag, err.Error())
				w.ko.Add(1.0)
			} else {
				w.tx.Add(float64(nmetrics))
				w.timings.Observe(float64(time.Since(tstart)))
			}

		}

		slog.Infof("%s: stopped", w.tag)

		w.wg.Done()
	}()
}

func initWriter(){
	writer = NewWrite()
	writer.init()
}
