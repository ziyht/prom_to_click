package modules

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

var readerContent = []interface{}{"component", "reader"}

type clickReader struct {
	click   *click
	cfg     *ReaderCfg
	queries prometheus.Counter
	rows    prometheus.Counter
	tag     string
}

func (r *clickReader) init() {

	r.tag = "reader"

	r.cfg   = &Cfg.Reader
	r.click = Engine.clicks.GetServer(r.cfg.Clickhouse)

	if r.click == nil {
		slog.Fatalf("%s: the clickhouse '%s' set in reader can not be found", r.tag, r.cfg.Clickhouse)
	}

	// check and set default vals
	if r.cfg.MaxSamples == 0 {
		r.cfg.MaxSamples = 11000
	}

	if r.cfg.Quantile == 0 {
		r.cfg.Quantile = 0.75
	} else if r.cfg.Quantile < 0.0 {
		r.cfg.Quantile = 0.0
	} else if r.cfg.Quantile > 1.0 {
		r.cfg.Quantile = 1.0
	}

	if r.cfg.MinStep <= 0 {
		r.cfg.MinStep = 15
	}
}

func (r *clickReader) IsHealthy() bool {
	return r.click.IsHealthy()
}

func (r *clickReader) HandlePromReadReq(req *remote.ReadRequest, hr *http.Request) (*remote.ReadResponse, error) {

	var err error

	resp := remote.ReadResponse{
		Results: []*remote.QueryResult{
			{Timeseries: make([]*remote.TimeSeries, 0, 0)},
		},
	}

	// need to map tags to timeseries to record samples
	var tsres = make(map[string]*remote.TimeSeries)

	// for Debugfging/figuring out query format/etc
	rcount := 0
	tag    := r.tag
	tStart := time.Now()
	for _, query := range req.Queries {

		// get the select sql
		q := r.getSqlQuery(query, hr)
		if q == nil{
			return &resp, err
		}
		slog.Debugf("%s: query: running sql: %s", q.tag, q.sql)
		tag = q.tag

		// todo: metrics on number of errors, rows, selects, timings, etc
		cStart := time.Now()
		rows, err := r.click.Query(q.sql)
		if err != nil {
			slog.Errorf("%s: query sql failed: %s: %s", q.tag, q.sql, err)
			return &resp, err
		}

		defer rows.Close()

		curRCount := 0
		// build map of timeseries from sql result
		for rows.Next() {
			curRCount++
			var (
				cnt   int
				t     int64
				name  string
				tags  []string
				value float64
			)
			if err = rows.Scan(&cnt, &t, &name, &tags, &value); err != nil {
				slog.Errorf("%s: scan: %s", q.tag, err.Error())
			}

			// debug
			//fmt.Printf(fmt.Sprintf("%d,%d,%s,%s,%f\n", cnt, t, name, strings.Join(tags, ":"), value))

			// borrowed from influx remote storage adapter - array sep
			key := strings.Join(tags, "\xff")
			ts, ok := tsres[key]
			if !ok {
				ts = &remote.TimeSeries{
					Labels: makeLabels(tags),
				}
				tsres[key] = ts
			}
			ts.Samples = append(ts.Samples, &remote.Sample{
				Value       : value,
				TimestampMs : t,
			})
		}

		rcount += curRCount

		slog.Debugf("%s: returned %d rows, cost: %s", q.tag, curRCount, time.Now().Sub(cStart).String())
	}

	// now add results to response
	for _, ts := range tsres {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}

	slog.Infof("%s: query: returning %d rows for %d queries, cost: %s", tag, rcount, len(req.Queries), time.Now().Sub(tStart).String())

	return &resp, nil
}

func (r *clickReader) getSqlQuery(query *remote.Query, hr *http.Request) *sqlQuery {

	q := newSqlQuery(query)
	q.tag = r.tag + ": " + q.tag

	hr.ParseForm()

	dbName := r.click.cfg.Database
	tbName := r.click.cfg.Table
	{
		args, ok := hr.Form["db"]
		if ok {
			dbName = args[0]
		}
	}
	{
		args, ok := hr.Form["table"]
		if ok {
			tbName = args[0]
		}
	}
	q.tag = r.tag + "<-" + r.click.tag + "/" + dbName + "." + tbName

	// valid time period checker
	if query.EndTimestampMs < query.StartTimestampMs {
		slog.Errorf("%s: Start time is after end time", q.tag)
	}

	q.iStart = query.StartTimestampMs / 1000
	q.iEnd   = query.EndTimestampMs   / 1000
	q.sStart = time.Unix(q.iStart, 0).Format("2006-01-02 03:04:05")
	q.sEnd   = time.Unix(q.iEnd  , 0).Format("2006-01-02 03:04:05")
	q.sDate  = time.Unix(q.iStart, 0).Format("2006-01-02")

	period := q.iEnd - q.iStart
	step := period / int64(r.cfg.MaxSamples)
	if step < int64(r.cfg.MinStep) {
		step = int64(r.cfg.MinStep)
	}

	q.rows = append(q.rows, fmt.Sprintf("COUNT() AS CNT, (intDiv(toUInt32(ts), %d) * %d) * 1000 as t", step, step))
	q.rows = append(q.rows, "name", "tags")
	q.rows = append(q.rows, fmt.Sprintf("quantile(%f)(val) as value", r.cfg.Quantile))

	q.from = fmt.Sprintf("%s.%s", dbName, tbName)

	q.wheres = append(q.wheres, fmt.Sprintf("date >= '%s' AND ts >= '%s' AND ts <= '%s'", q.sDate, q.sStart, q.sEnd))
	q.wheres = append(q.wheres, r.getMatchWheres(query)...)

	q.groupby = "t, name, tags"
	q.orderby = "tags"

	q.genSql()

	return q
}

func (r *clickReader) getMatchWheres(query *remote.Query) []string {

	var matchWheres []string

	for _, m := range query.Matchers {
		// __name__ is handled specially - match it directly
		// as it is stored in the name column (it's also in tags as __name__)
		// note to self: add name to index.. otherwise this will be slow..
		if m.Name == model.MetricNameLabel {
			var whereAdd string
			switch m.Type {
			case remote.MatchType_EQUAL:
				whereAdd = fmt.Sprintf(` name='%s' `, strings.Replace(m.Value, `'`, `\'`, -1))
			case remote.MatchType_NOT_EQUAL:
				whereAdd = fmt.Sprintf(` name!='%s' `, strings.Replace(m.Value, `'`, `\'`, -1))
			case remote.MatchType_REGEX_MATCH:
				whereAdd = fmt.Sprintf(` match(name, %s) = 1 `, strings.Replace(m.Value, `/`, `\/`, -1))
			case remote.MatchType_REGEX_NO_MATCH:
				whereAdd = fmt.Sprintf(` match(name, %s) = 0 `, strings.Replace(m.Value, `/`, `\/`, -1))
			}
			matchWheres = append(matchWheres, whereAdd)
			continue
		}

		switch m.Type {
		case remote.MatchType_EQUAL:
			var insql bytes.Buffer
			asql := "arrayExists(x -> x IN (%s), tags) = 1"
			// value appears to be | sep'd for multiple matches
			for i, val := range strings.Split(m.Value, "|") {
				if len(val) < 1 {
					continue
				}
				if i == 0 {
					istr := fmt.Sprintf(`'%s=%s' `, m.Name, strings.Replace(val, `'`, `\'`, -1))
					insql.WriteString(istr)
				} else {
					istr := fmt.Sprintf(`,'%s=%s' `, m.Name, strings.Replace(val, `'`, `\'`, -1))
					insql.WriteString(istr)
				}
			}
			wstr := fmt.Sprintf(asql, insql.String())
			matchWheres = append(matchWheres, wstr)

		case remote.MatchType_NOT_EQUAL:
			var insql bytes.Buffer
			asql := "arrayExists(x -> x IN (%s), tags) = 0"
			// value appears to be | sep'd for multiple matches
			for i, val := range strings.Split(m.Value, "|") {
				if len(val) < 1 {
					continue
				}
				if i == 0 {
					istr := fmt.Sprintf(`'%s=%s' `, m.Name, strings.Replace(val, `'`, `\'`, -1))
					insql.WriteString(istr)
				} else {
					istr := fmt.Sprintf(`,'%s=%s' `, m.Name, strings.Replace(val, `'`, `\'`, -1))
					insql.WriteString(istr)
				}
			}
			wstr := fmt.Sprintf(asql, insql.String())
			matchWheres = append(matchWheres, wstr)

		case remote.MatchType_REGEX_MATCH:
			asql := `arrayExists(x -> 1 == match(x, '^%s=%s'),tags) = 1`
			// we can't have ^ in the regexp since keys are stored in arrays of key=value
			if strings.HasPrefix(m.Value, "^") {
				val := strings.Replace(m.Value, "^", "", 1)
				val = strings.Replace(val, `/`, `\/`, -1)
				matchWheres = append(matchWheres, fmt.Sprintf(asql, m.Name, val))
			} else {
				val := strings.Replace(m.Value, `/`, `\/`, -1)
				matchWheres = append(matchWheres, fmt.Sprintf(asql, m.Name, val))
			}

		case remote.MatchType_REGEX_NO_MATCH:
			asql := `arrayExists(x -> 1 == match(x, '^%s=%s'),tags) = 0`
			if strings.HasPrefix(m.Value, "^") {
				val := strings.Replace(m.Value, "^", "", 1)
				val = strings.Replace(val, `/`, `\/`, -1)
				matchWheres = append(matchWheres, fmt.Sprintf(asql, m.Name, val))
			} else {
				val := strings.Replace(m.Value, `/`, `\/`, -1)
				matchWheres = append(matchWheres, fmt.Sprintf(asql, m.Name, val))
			}
		}
	}

	return matchWheres
}