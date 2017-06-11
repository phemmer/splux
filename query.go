package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/phemmer/splux/errors"
	"github.com/phemmer/splux/splunk"

	influxdb "github.com/influxdata/influxdb-client"
	"github.com/influxdata/influxdb/influxql"
)

var INFLUXDB_ADDR = "http://192.168.0.70:8086"

type InfluxCmd struct {
}

func (ic *InfluxCmd) Getinfo() map[string]interface{} {
	return map[string]interface{}{
		"type":                "reporting",
		"generating":          true,
		"generates_timeorder": true,
	}
}
func (ic *InfluxCmd) Execute(si splunk.Searchinfo) (splunk.Chunker, error) {
	var db string

	var args []string
	for i, arg := range si.Args {
		parts := strings.SplitN(arg, "=", 2)
		// stop param parsing on first arg not containing '=', or where key contains whitespace.
		if len(parts) != 2 || strings.Contains(parts[0], " ") {
			args = append(args, si.Args[i:]...)
			break
		}

		switch parts[0] {
		case "db":
			db = parts[1]
		default:
			return nil, fmt.Errorf("unsupported parameter '%s'", parts[0])
		}
	}

	if len(args) == 0 {
		return nil, fmt.Errorf("query string missing")
	}
	qStr := args[0]
	//qArgs := args[1:]

	var err error
	qStr = qWhere(qStr, "time >= $tMin AND time <= $tMax")
	qStr = qOrder(qStr, "time desc")

	client, err := influxdb.NewClient(INFLUXDB_ADDR)
	if err != nil {
		return nil, err
	}

	querier := client.Querier()
	querier.Database = db
	querier.Chunked = true

	return newChunker(querier, qStr, si.EarliestTime, si.LatestTime)
}

func qWhere(qStr, clause string) string {
	scnr := influxql.NewScanner(bytes.NewBufferString(qStr))
SCANNER:
	for {
		tok, pos, _ := scnr.Scan()
		switch tok {
		case influxql.EOF:
			break SCANNER
		case influxql.WHERE:
			return qStr[:pos.Char+5] + " " + clause + " AND" + qStr[pos.Char+5:]
		case influxql.GROUP, influxql.ORDER, influxql.LIMIT, influxql.OFFSET, influxql.SLIMIT, influxql.SOFFSET:
			return qStr[:pos.Char] + "WHERE " + clause + " " + qStr[pos.Char:]
		}
	}
	return qStr + " WHERE " + clause
}

func qOrder(qStr, clause string) string {
	scnr := influxql.NewScanner(bytes.NewBufferString(qStr))
	haveOrder := false
SCANNER:
	for {
		tok, pos, _ := scnr.Scan()
		switch tok {
		case influxql.EOF:
			break SCANNER
		case influxql.ORDER:
			haveOrder = true
		case influxql.BY:
			if !haveOrder {
				continue SCANNER
			}
			return qStr[:pos.Char+2] + " " + clause + ", " + qStr[pos.Char+2:]
		case influxql.LIMIT, influxql.OFFSET, influxql.SLIMIT, influxql.SOFFSET:
			return qStr[:pos.Char] + "ORDER BY " + clause + qStr[pos.Char:]
		}
	}
	return qStr + " ORDER BY " + clause
}

type Chunker struct {
	influxdb.Cursor
	curSet influxdb.ResultSet
}

func newChunker(querier *influxdb.Querier, qStr string, tMin, tMax time.Time) (*Chunker, error) {
	cur, err := querier.Select(qStr,
		influxdb.Param("tMin", tMin.UnixNano()),
		influxdb.Param("tMax", tMax.UnixNano()),
	)
	if err != nil {
		return nil, errors.F(err, "performing query %q", qStr)
	}

	return &Chunker{Cursor: cur}, nil
}

func (ch *Chunker) NextChunk() ([]string, [][]interface{}, error) {
	if ch.curSet == nil {
		var err error
		if ch.curSet, err = ch.Cursor.NextSet(); err != nil {
			if errors.IsEOF(err) {
				return nil, nil, nil
			}
			return nil, nil, errors.F(err, "retrieving next set")
		}
		if ch.curSet == nil {
			return nil, nil, nil
		}
	}

	ser, err := ch.curSet.NextSeries()
	if err != nil {
		if errors.IsEOF(err) {
			ch.curSet = nil
			return []string{}, nil, nil
		}
		return nil, nil, errors.F(err, "retrieving next series")
	}

	cols := []string{"_time", "measurement"}
	staticFields := []interface{}{ser.Name()}

	for _, tag := range ser.Tags() {
		cols = append(cols, tag.Key)
		staticFields = append(staticFields, tag.Value)
	}

	var timeIdx int // we already have the time, we don't need it again
	for i, col := range ser.Columns() {
		if col == "time" {
			timeIdx = i
		} else {
			cols = append(cols, col)
		}
	}

	var data [][]interface{}
	for {
		row, err := ser.NextRow()
		if err != nil {
			if errors.IsEOF(err) {
				break
			}
			return nil, nil, errors.F(err, "retrieving next row")
		}

		fields := make([]interface{}, len(cols))
		fields[0] = fmt.Sprintf("%.3f", float64(row.Time().UnixNano())/float64(time.Second))
		copy(fields[1:], staticFields)
		values := row.Values()
		values = append(values[:timeIdx], values[timeIdx+1:]...)
		copy(fields[1+len(staticFields):], values)
		data = append(data, fields)
	}

	return cols, data, nil
}

func (ch *Chunker) Close() {
	ch.Cursor.Close()
	ch.Cursor = nil
}

func main() {
	os.Exit(Main())
}

func Main() int {
	if err := splunk.NewProcessor(
		&InfluxCmd{},
		os.Stdin,
		os.Stdout,
	).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		return 1
	}

	return 0
}
