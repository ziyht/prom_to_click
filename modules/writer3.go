package modules

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/emirpasic/gods/utils"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"net/http"
	"regexp"
	"sort"
	"sync"
	"time"

	rbt "github.com/emirpasic/gods/trees/redblacktree"

	"github.com/prometheus/client_golang/prometheus"
)

type promSample3 struct {
	name        string
	tags        []string
	val         float64
	ts          time.Time
	fingerprint uint64
	isSample    bool
}

type fingerprintCheckpoint struct {
	firstDiscovery time.Time
	//lastDiscovery  time.Time
	fingerprint    uint64
}

type fingerprintCache struct {
	fingerprints map[uint64]*fingerprintCheckpoint
	timelines    *rbt.Tree
	holdTime     time.Duration
}

func newFingerprintCache(holdSecond int) *fingerprintCache {
	out := new(fingerprintCache)

	if holdSecond < 0 {
		holdSecond = 0
	}

	out.fingerprints = map[uint64]*fingerprintCheckpoint{}
	out.timelines    = rbt.NewWith(utils.TimeComparator)
	out.holdTime     = time.Second * time.Duration(holdSecond)

	return out
}

// return true if is a new one
func (fc *fingerprintCache) cache(fingerprint uint64) bool {

	newOne := false

	now := time.Now()

	fcp, exist := fc.fingerprints[fingerprint]
	if ! exist {
		fcp = &fingerprintCheckpoint{now, fingerprint}

		fc.fingerprints[fingerprint] = fcp
		fc.timelines.Put(fcp.firstDiscovery, fcp)

		newOne = true
	} else {

	}

	return newOne
}

func (fc *fingerprintCache)Shrink() int {

	now := time.Now()
	var removes []*fingerprintCheckpoint

	iter := fc.timelines.Iterator()
	for iter.Next() {

		fcp := iter.Value().(*fingerprintCheckpoint)

		// remove cache created by prev day
		if now.Day() != fcp.firstDiscovery.Day() {
			removes = append(removes, fcp)
			continue
		}

		// remove cache exceed deadline
		if now.Sub(fcp.firstDiscovery) > fc.holdTime {
			removes = append(removes, fcp)
		} else {
			break
		}
	}

	for _, fcp := range removes {
		delete(fc.fingerprints, fcp.fingerprint)
		fc.timelines.Remove(fcp.firstDiscovery)
	}

	return len(removes)
}

type clickOutput3 struct {
	tag          		string
	cw                  *clickWriter3
	db           		string
	tableMetrics     	string
	tableSamples        string
	writeCounter       	prometheus.Counter
	writeFailedCounter 	prometheus.Counter
	test     			prometheus.Counter
	timings  			prometheus.Histogram
	totalRecv			uint64
	totalWrite          uint64
	inputs   			chan *promSample3
	fingerprints        *fingerprintCache
	fingerprintsMu      sync.Mutex
}

func NewClickOutput3(cw *clickWriter3, db string, table string) (out *clickOutput3) {

	out = new(clickOutput3)

	out.cw           = cw
	out.db           = db
	out.tableMetrics = table + "_metrics"
	out.tableSamples = table + "_samples"
	out.tag          = cw.tag + "->" + cw.click.tag + "/" + db + ".[" + out.tableMetrics + "," + out.tableSamples + "]"

	out.inputs       = make(chan *promSample3, cw.cfg.Buffer)
	out.fingerprints = newFingerprintCache(60 * 60 * 24)

	return out
}

func (co *clickOutput3) HandleError(err error) error {

	compile, _ := regexp.Compile("(Database|Table) .* doesn't exist")

	if compile.Match([]byte(err.Error())) {
		return co.cw.TryCreateDatabaseTable(co)
	} else {
		return err
	}

	return nil
}

func (co *clickOutput3)Stop(){
	close(co.inputs)
}

func (co *clickOutput3) Start() {

	w := co.cw

	wait := w.cfg.Wait

	sigSample := new(promSample3)

	// wake up writer fo every 1 seconds
	go func() {

		for {
			time.Sleep(time.Second)
			select {
			case co.inputs <- sigSample:
			default:
			}
		}

	}()

	var insertMetricSQL = `INSERT INTO %s.%s (name, tags, fingerprint) VALUES (?, ?, ?)`
	var insertSampleSQL = `INSERT INTO %s.%s (fingerprint, ts, val) VALUES (?, ?, ?)`

	go func() {
		w.wg.Add(1)
		slog.Infof("%s: started", co.tag)

		sql1    := fmt.Sprintf(insertMetricSQL, co.db, co.tableMetrics)
		sql2    := fmt.Sprintf(insertSampleSQL, co.db, co.tableSamples)
		chanOK := true

		var metrics []*promSample3
		var samples []*promSample3

		for chanOK {
			w.test.Add(1)

			// get next batch of requests
			tstart := time.Now()
			for i := 0; i < w.cfg.Batch; i++ {
				var req *promSample3

				// get request and also check if channel is closed
				req, chanOK = <-co.inputs

				if !chanOK {
					slog.Infof("%s: stopping...", co.tag)
					break
				}

				// we need to filter out sigSample
				if req != sigSample{
					if ! req.isSample {
						metrics = append(metrics, req)
					} else {
						samples = append(samples, req)
					}
				}

				if wait > 0 && time.Now().Sub(tstart) > time.Second * time.Duration(wait) {
					break
				}
			}

			// ensure we have something to send..
			nmetrics := len(metrics)
			nsamples := len(samples)
			if (nmetrics + nsamples) < 1 {
				continue
			}

			// do for samples
			if nsamples > 0 {
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
				smt, err := tx.Prepare(sql2)
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

						smt, err = tx.Prepare(sql2)
						if err != nil {
							tx.Commit()

							slog.Errorf("%s: prepare statement: %s", co.tag, err.Error())
							continue
						}
					}
				}

				for _, sp := range samples {
					// ensure tags are inserted in the same order each time
					// possibly/probably impacts indexing?

					_, err = smt.Exec(sp.fingerprint, sp.ts, sp.val)

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
					samples = []*promSample3{}				// write ok, clear reqs

					w.totalWrite += uint64(nsamples)

					slog.Infof("%s: write %d samples, total: %d, cost: %s", co.tag, nsamples, w.totalWrite, time.Now().Sub(start).String())

					w.writeCounter.Add(float64(nmetrics))
					w.timings.Observe(float64(time.Since(tstart)))
				}
			}

			// do for metrics
			if nmetrics > 0 {
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
				smt, err := tx.Prepare(sql1)
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

						smt, err = tx.Prepare(sql1)
						if err != nil {
							tx.Commit()

							slog.Errorf("%s: prepare statement: %s", co.tag, err.Error())
							continue
						}
					}
				}

				for _, mt := range metrics {
					// ensure tags are inserted in the same order each time
					// possibly/probably impacts indexing?

					_, err = smt.Exec(mt.name, clickhouse.Array(mt.tags), mt.fingerprint)

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
					metrics = []*promSample3{}				// write ok, clear reqs

					// w.totalWrite += uint64(nmetrics)     // ...

					slog.Infof("%s: write %d metrics, total: %d, cost: %s", co.tag, nmetrics, w.totalWrite, time.Now().Sub(start).String())

					w.timings.Observe(float64(time.Since(tstart)))
				}
			}
		}

		slog.Infof("%s: stopped", co.tag)

		w.wg.Done()
	}()
}

type clickWriter3 struct {
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
	outputs             map[string]*clickOutput3
}

func (w *clickWriter3)init(){

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

	w.outputs = map[string]*clickOutput3{}
}

func (w *clickWriter3) Start() {
	return // do nothing is ok
}

func (w *clickWriter3) IsHealthy() bool {
	return w.click.IsHealthy()
}

func (w *clickWriter3) HandleError(err error, co *clickOutput3) error {

	compile, _ := regexp.Compile("(Database|Table) .* doesn't exist")

	if compile.Match([]byte(err.Error())) {
		return w.TryCreateDatabaseTable(co)
	}

	return err
}

func (w *clickWriter3) TryCreateDatabaseTable(co *clickOutput3) error{


	creatDBSql     := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", co.db)

	creatTableSql1 := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s.%s (
			date        Date      DEFAULT toDate(now()),
            name        String,
            tags        Array(String),
			fingerprint UInt64
		)
		ENGINE = ReplacingMergeTree
			PARTITION BY toYYYYMM(date)
			ORDER BY (date, name, tags, fingerprint)`, co.db, co.tableMetrics)

	creatTableSql2 := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
			fingerprint  UInt64,
			ts           DateTime,
			val          Float64
		)
		ENGINE = MergeTree
			PARTITION BY toYYYYMM(ts)
			ORDER BY (fingerprint, ts)`, co.db, co.tableSamples)

	{
		_, err := w.click.Exec(creatDBSql)
		if err != nil{
			return err
		}
	}

	{
		_, err := w.click.Exec(creatTableSql1)
		if err != nil{
			return err
		}
	}

	{
		_, err := w.click.Exec(creatTableSql2)
		if err != nil{
			return err
		}
	}

	return nil
}

func (w *clickWriter3)getClickOutput(r *http.Request) (*clickOutput3, error){
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}

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
		co = NewClickOutput3(w, dbName, tbName)
		co.Start()

		w.outputs[coName] = co
	}

	return co, nil
}

func (w *clickWriter3)HandlePromWriteReq(req *remote.WriteRequest, r *http.Request) (*remote.ReadResponse, error){

	curRecvs := 0

	co, err := w.getClickOutput(r)
	if err != nil{
		slog.Errorf("%s: get clickOutput failed: %s", w.tag, err)
		return nil, nil
	}

	fingerprints := make(map[uint64]*promSample3, len(req.Timeseries))

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

		// calculate fingerprints
		// we do not cache fingerprint now, because we need to update the date of metric in clickhouse
		labels := series.Labels
		sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })
		fingerprint := Fingerprint(labels)

		sort.Strings(tags)

		for _, sample := range series.Samples {
			sp := new(promSample3)
			sp.name        = name
			sp.ts          = time.Unix(sample.TimestampMs/1000, sample.TimestampMs % 1000)
			sp.val         = sample.Value
			sp.tags        = tags
			sp.fingerprint = fingerprint
			sp.isSample    = true

			co.inputs <- sp
		}

		{
			sp := new(promSample3)
			sp.name        = name
			sp.tags        = tags
            sp.fingerprint = fingerprint

			fingerprints[fingerprint] = sp		// here set tags, we do not convert to json
		}
	}

	{
		// send new metrics
		co.fingerprintsMu.Lock()
		for _, sp := range fingerprints{
			if co.fingerprints.cache(sp.fingerprint) {
				co.inputs <- sp
			}
		}

		removed := co.fingerprints.Shrink()
		if removed > 0 {
			slog.Debugf("%s: removed %d timeout fingerprint from cache", co.tag, removed)
		}

		co.fingerprintsMu.Unlock()
	}

	w.totalRecv  += uint64(curRecvs)
	co.totalRecv += uint64(curRecvs)

	Engine.server.recvCounter.Add(float64(curRecvs))
	slog.Infof("%s: received %d samples, total: %d", co.tag, curRecvs, co.totalRecv)

	return nil, nil
}

func (w *clickWriter3) Stop() {

	for _, co := range w.outputs {
		co.Stop()
	}
}

func (w *clickWriter3) Wait() {
	w.wg.Wait()
}