package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/phemmer/splux/errors"
	"github.com/phemmer/splux/splunk"

	influxdb "github.com/influxdata/influxdb-client"
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

	for i, arg := range si.Args {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			si.Args = si.Args[i:]
			si.RawArgs = si.RawArgs[i:]
			break
		}
		switch parts[0] {
		case "db":
			db = parts[1]
		default:
			return nil, fmt.Errorf("unsupported parameter '%s'", parts[0])
		}
	}

	var args []string
	for i, arg := range si.Args {
		if strings.ToLower(arg) == "where" {
			args := si.RawArgs[0 : i+1]
			args = append(args,
				"time", ">=", "$tMin",
				"AND",
				"time", "<=", "$tMax",
				"AND",
			)
			args = append(args, si.RawArgs[i+1:]...)
			break
		}
		if strings.ToLower(arg) == "group" {
			args = si.RawArgs[0:i]
			args = append(args,
				"WHERE",
				"time", ">=", "$tMin",
				"AND",
				"time", "<=", "$tMax",
			)
			args = append(args, si.RawArgs[i:]...)
			break
		}
	}
	if len(args) == 0 {
		args = append(si.RawArgs,
			"WHERE",
			"time", ">=", "$tMin",
			"AND",
			"time", "<=", "$tMax",
		)
	}

	qStr := strings.Join(args, " ")

	client, err := influxdb.NewClient(INFLUXDB_ADDR)
	if err != nil {
		return nil, err
	}

	querier := client.Querier()
	querier.Database = db

	return newChunker(querier, qStr, si.EarliestTime, si.LatestTime)
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
		return nil, errors.F(err, "performing query")
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
			return nil, nil, nil
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
			return nil, nil, err
		}

		fields := make([]interface{}, len(cols))
		fields[0] = fmt.Sprintf("%.3f", float64(row.Time().UnixNano())/float64(time.Second))
		copy(fields[1:], staticFields)
		values := row.Values()
		values = append(values[:timeIdx], values[timeIdx+1:]...)
		copy(fields[1+len(staticFields):], values)
		data = append(data, fields)
	}

	ch.curSet = nil
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
