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

type clickReader3 struct {
	click   *click
	cfg     *ReaderCfg
	queries prometheus.Counter
	rows    prometheus.Counter
	tag     string
}

func (r *clickReader3) init() {
	r.tag = "reader3"

	r.cfg   = &Cfg.Reader
	r.click = Engine.clicks.GetServer(r.cfg.Clickhouse)

	if r.click == nil {
		slog.Fatalf("the clickhouse '%s' set in reader can not be found", r.cfg.Clickhouse)
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

func (r *clickReader3) IsHealthy() bool {
	return r.click.IsHealthy()
}

func (r *clickReader3) HandlePromReadReq(req *remote.ReadRequest, hr *http.Request) (*remote.ReadResponse, error) {

	var err error

	resp := remote.ReadResponse{
		Results: []*remote.QueryResult{
			{Timeseries: make([]*remote.TimeSeries, 0, 0)},
		},
	}

	// need to map tags to timeseries to record samples
	var tsres = make(map[uint64]*remote.TimeSeries)

	var (
		t        	 int64
		tags     	 []string
		value    	 float64
		cnt      	 int
		rcount   	 int64			// row count
		scount   	 int64			// sample count
		lastTSms 	 int64 			// last timestamp
		lastFP  	 uint64
		lastLPs      []*remote.LabelPair
		lastTS  	 *remote.TimeSeries
		fingerprint  uint64
		fingerprints map[uint64][]*remote.LabelPair
		exist        bool
	)

	slog.Infof("%s: new query req: %d queries", r.tag, len(req.Queries))
	tStart := time.Now()

	tag := r.tag
	fingerprints = map[uint64][]*remote.LabelPair{}

	for _, query := range req.Queries {

		q1 := r.getSqlQuery(query, hr)
		q2 := r.getSqlQuery2(query, hr)
		if q1 == nil || q2 == nil{
			slog.Errorf("%s: gen (getSqlQuery)s failed", r.tag)
			return &resp, err
		}

		slog.Debugf("%s: query: running sql: %s", q1.tag, q1.sql)
		rows1, err1 := r.click.Query(q1.sql)
		if err1 != nil {
			slog.Errorf("%s: query sql failed: %s: %s", q1.tag, q1.sql, err1)
			return &resp, err1
		}
		slog.Debugf("%s: query: running sql: %s", q2.tag, q2.sql)
		rows2, err2 := r.click.Query(q2.sql)
		if err2 != nil {
			slog.Errorf("%s: query sql failed: %s: %s", q2.tag, q2.sql, err2)
			return &resp, err2
		}

		tag = q2.tag

		defer rows1.Close()
		defer rows2.Close()

		var (
			curRCount1 int64
			curSCount1 int64
		)

		// handle fingerprints and tags in rows1, parsing to LabelPair
		for rows1.Next(){
			curRCount1++

			if err = rows1.Scan(&cnt, &fingerprint, &tags); err != nil {
				slog.Errorf("%s: scan: %s", q1.tag, err.Error())
			}

			_, ok := fingerprints[fingerprint]
			if !ok {
				curSCount1 ++
				fingerprints[fingerprint] = makeLabels(tags)
			}
		}
		slog.Debugf("%s: returned %d rows, parsed %d metrics", q1.tag, curRCount1, curSCount1)

		var (
			curRCount2 int64
			curSCount2 int64
		)

		// build map of timeseries from sql result
		for rows2.Next() {
			curRCount2++

			if err = rows2.Scan(&fingerprint, &t, &value); err != nil {
				slog.Errorf("%s: scan: %s", q2.tag, err.Error())
			}

			// new query, order by tags,t, so the same tags will be returned together
			// so we can using the last tag and current tag to check if is new

			if fingerprint != lastFP{
				lastFP = fingerprint

				lastLPs, exist = fingerprints[fingerprint]
				if !exist {
					// this should not happen
					slog.Errorf("%s: invalid sample, fingerprint '%s' can not be found in query1", q2.tag, fingerprint)
				}

				// maybe a new tag, check and create new one
				ts, ok := tsres[fingerprint]
				if !ok {
					ts = &remote.TimeSeries{
						Labels: lastLPs,
					}
					tsres[fingerprint] = ts
				}

				lastTS   = ts
				lastTSms = 0
			} else if lastLPs == nil {
				continue		// skip invalid samples, of cause this should not happen like prev branch
			}

			// the same as last, append directly
			ts := lastTS
			if lastTSms != t{
				curSCount2++
				ts.Samples = append(ts.Samples, &remote.Sample{
					Value       : value,
					TimestampMs : t,
				})
			}
			lastTSms = t
		}

		slog.Debugf("%s: returned %d rows, wrapped %d samples", q2.tag, curRCount2, curSCount2)

		rcount += curRCount2
		scount += curSCount2
	}

	// now add results to response
	for _, ts := range tsres {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}

	slog.Infof("%s: query: returning %d rows for %d queries, wrapped: %d samples, cost: %s", tag, rcount, len(req.Queries), scount, time.Now().Sub(tStart).String())

	return &resp, nil
}

// first, we need to query the metrics needed, here we do not using inner join to return all result in one query
// because 1. it need more memory for clickhouse to do 'group by' and 'order by' operations
//         2. it transfer more data
func (r *clickReader3) getSqlQuery(query *remote.Query, hr *http.Request) *sqlQuery {

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
			tbName = args[0] + "_metrics"
		}
	}
	tbNameMetrics := tbName + "_metrics"
	q.tag = r.click.tag + "/" + dbName + "." + tbNameMetrics

	q.iStart = query.StartTimestampMs / 1000
	q.iEnd   = query.EndTimestampMs   / 1000
	if r.cfg.Utc{
		q.sStart      = time.Unix(q.iStart, 0).UTC().Format("2006-01-02 15:04:05")
		q.sEnd        = time.Unix(q.iEnd  , 0).UTC().Format("2006-01-02 15:04:05")
		q.sStartDate  = time.Unix(q.iStart, 0).UTC().Format("2006-01-02")
		q.sEndDate    = time.Unix(q.iEnd  , 0).UTC().Format("2006-01-02")
	} else {
		q.sStart      = time.Unix(q.iStart, 0).Format("2006-01-02 15:04:05")
		q.sEnd        = time.Unix(q.iEnd  , 0).Format("2006-01-02 15:04:05")
		q.sStartDate  = time.Unix(q.iStart, 0).Format("2006-01-02")
		q.sEndDate    = time.Unix(q.iEnd  , 0).Format("2006-01-02")
	}


	// target sql like: select count() as cnt, fingerprint, tags from mode3_metrics where has(tags, '__name__=up') from <db>.<table> group by fingerprint, tags

	q.rows = append(q.rows, "count() as cnt, fingerprint, tags")

	q.from = fmt.Sprintf("%s.%s", dbName, tbNameMetrics)

	q.wheres = append(q.wheres, fmt.Sprintf("date >= '%s' AND date <= '%s'", q.sStartDate, q.sEndDate) )
	q.wheres = append(q.wheres, r.getMatchWheres(query)...)

	q.groupBy = "fingerprint, tags"
	q.orderBy = ""

	q.genSql()

	return q
}

func (r *clickReader3) getSqlQuery2(query *remote.Query, hr *http.Request) *sqlQuery {
	q := newSqlQuery(query)
	q.tag = r.tag + ": " + q.tag

	hr.ParseForm()

	dbName        := r.click.cfg.Database
	tbName        := r.click.cfg.Table
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
	tbNameMetrics := tbName + "_metrics"
	tbNameSamples := tbName + "_samples"
	q.tag = r.click.tag + "/" + dbName + ".[" + tbNameMetrics + "," + tbNameSamples + "]"

	// target sql like:
	// select * from mode3_samples inner join (select fingerprint from mode3_metrics where name == 'up' group by fingerprint) using fingerprint group by fingerprint, ts order by ts

	// valid time period checker
	if query.EndTimestampMs < query.StartTimestampMs {
		slog.Errorf("%s: Start time is after end time", q.tag)
		return nil
	}

	q.iStart = query.StartTimestampMs / 1000
	q.iEnd   = query.EndTimestampMs   / 1000
	if r.cfg.Utc{
		q.sStart      = time.Unix(q.iStart, 0).UTC().Format("2006-01-02 15:04:05")
		q.sEnd        = time.Unix(q.iEnd  , 0).UTC().Format("2006-01-02 15:04:05")
		q.sStartDate  = time.Unix(q.iStart, 0).UTC().Format("2006-01-02")
		q.sEndDate    = time.Unix(q.iEnd  , 0).UTC().Format("2006-01-02")
	} else {
		q.sStart      = time.Unix(q.iStart, 0).Format("2006-01-02 15:04:05")
		q.sEnd        = time.Unix(q.iEnd  , 0).Format("2006-01-02 15:04:05")
		q.sStartDate  = time.Unix(q.iStart, 0).Format("2006-01-02")
		q.sEndDate    = time.Unix(q.iEnd  , 0).Format("2006-01-02")
	}

	period := q.iEnd - q.iStart
	step := period / int64(r.cfg.MaxSamples)
	if step < int64(r.cfg.MinStep) {
		step = int64(r.cfg.MinStep)
	}

	q.rows = append(q.rows, "fingerprint")
	q.rows = append(q.rows, fmt.Sprintf("(intDiv(toUInt32(ts), %d) * %d) * 1000 as t", step, step))
	q.rows = append(q.rows, "anyLast(val) as value")

	q.from = fmt.Sprintf("%s.%s", dbName, tbNameSamples)

	nameWhere := r.getWhereName(query)
	var wheres []string
	if len(nameWhere) > 0 {
		wheres = append(wheres, nameWhere)
		wheres = append(wheres, fmt.Sprintf("date >= '%s' AND date <= '%s'", q.sStartDate, q.sEndDate))
	}

	inSQL := fmt.Sprintf("fingerprint in (select fingerprint from %s.%s where %s group by fingerprint)", dbName, tbNameMetrics, strings.Join(wheres," AND "))

	q.wheres = append(q.wheres, fmt.Sprintf("ts >= '%s' AND ts <= '%s'", q.sStart, q.sEnd))
	q.wheres = append(q.wheres, inSQL)

	q.groupBy = "fingerprint, t"
	q.orderBy = "fingerprint, t"

	q.genSql()

	return q
}

func (r *clickReader3) getWhereName(query *remote.Query) string {
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

			return whereAdd
		}
	}

	return ""
}


func (r *clickReader3) getMatchWheres(query *remote.Query) []string  {

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
