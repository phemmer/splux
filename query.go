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

	if si.EarliestTime.UnixNano() == 0 && si.LatestTime.UnixNano() == 0 {
		return &RealTimeChunker{
			querier: querier,
			qStr:    qStr,
		}, nil
	}

	return newChunker(querier, qStr, si.EarliestTime, si.LatestTime)
}

type RealTimeChunker struct {
	querier *influxdb.Querier
	qStr    string

	lastTMax time.Time
	chunker  *Chunker
}

func (rtch *RealTimeChunker) NextChunk() ([]string, [][]interface{}, error) {
	//FIXME Bad implementation!
	// This is going to search for results since the last poll. However if results are getting inserted in batches, say every 10 seconds, and we're polling every 1 second, then points can be inserted in the past, beyond the start of our query time window.
	//
	// Don't really have a good solution for this :-(
	// The obvious solution of setting rtch.lastTMax to the time of the last point found is better, but still broken. If you have multiple inserters running on different intervals, you can still end up with points getting inserted in the past.
	if rtch.chunker == nil {
		tNow := time.Now()
		tLast := rtch.lastTMax
		if tLast.IsZero() {
			tLast = tNow.Add(-time.Minute)
		}
		sleepTime := time.Second - tNow.Sub(tLast)
		if sleepTime > 0 {
			time.Sleep(sleepTime)
		}

		tLast = tLast.Add(time.Nanosecond)
		var err error
		if rtch.chunker, err = newChunker(rtch.querier, rtch.qStr, tLast, tNow); err != nil {
			return nil, nil, errors.F(err, "executing real-time query")
		}
		rtch.lastTMax = tNow
	}

	cols, data, err := rtch.chunker.NextChunk()
	if err != nil {
		return nil, nil, errors.F(err, "retrieving real-time chunk")
	}
	if cols == nil {
		rtch.chunker.Close()
		rtch.chunker = nil
		// we could return empty and let next call re-init
		return rtch.NextChunk()
	}
	return cols, data, nil
}

func (rtch *RealTimeChunker) Close() {
	if rtch.chunker != nil {
		rtch.chunker.Close()
		rtch.chunker = nil
	}
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
