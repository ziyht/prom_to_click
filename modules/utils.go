package modules

import (
	"fmt"
	"github.com/prometheus/prometheus/storage/remote"
	"strings"
)

func makeLabels(tags []string) []*remote.LabelPair {
	pairs := make([]*remote.LabelPair, 0, len(tags))
	// (currently) writer includes __name__ in tags so no need to add it here
	// may change this to save space later..
	for _, tag := range tags {
		vals := strings.SplitN(tag, "=", 2)
		if len(vals) != 2 {
			slog.Errorf("Error unpacking tag key/val: %s\n", tag)
			continue
		}
		if vals[1] == "" {
			continue
		}
		pairs = append(pairs, &remote.LabelPair{
			Name:  vals[0],
			Value: vals[1],
		})
	}
	return pairs
}

type sqlQuery struct{
	// input
	query   *remote.Query

	// -- middle
	tag    string
	iStart int64
	iEnd   int64
	sDate  string
	sStart string
	sEnd   string

	// query
	sql      string
	rows     []string
	wheres   []string
	from     string
	orderby  string
	groupby  string
}

var queryCounter int64
func newSqlQuery(query *remote.Query) *sqlQuery{

	queryCounter++

	out := new(sqlQuery)

	out.tag   = fmt.Sprintf("query%d", queryCounter)
	out.query = query

	return out
}

func (q *sqlQuery)genSql(){

	q.sql = fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(q.rows, ", "),
		q.from,
		strings.Join(q.wheres, " AND "),
	)

	if q.groupby != "" {
		q.sql += " GROUP BY " + q.groupby
	}

	if q.orderby != "" {
		q.sql += " ORDER BY " + q.orderby
	}
}
