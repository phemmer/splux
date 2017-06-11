package splunk

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/phemmer/splux/errors"
)

type Command interface {
	Getinfo() map[string]interface{}
	Execute(Searchinfo) (Chunker, error)
}

type Chunker interface {
	NextChunk() ([]string, [][]interface{}, error)
	Close()
}

type Processor struct {
	Command Command

	input  *bufio.Reader
	output io.Writer

	Metadata Metadata
}

type Metadata struct {
	Action                      string
	Preview                     bool
	StreamingCommandWillRestart bool
	Searchinfo                  Searchinfo
}

type Searchinfo struct {
	Args           []string
	RawArgs        []string `json:"raw_args"`
	DispatchDir    string   `json:"dispatch_dir"`
	SID            string   `json:"sid"`
	App            string
	Owner          string
	Username       string
	SessionKey     string `json:"session_key"`
	SplunkdURI     string `json:"splunkd_uri"`
	SplunkdVersion string `json:"splunkd_version"`
	Search         string
	Command        string
	MaxResultRows  uint      `json:"maxresultrows"`
	EarliestTime   time.Time `json:"-"`
	LatestTime     time.Time `json:"-"`
}

func (si *Searchinfo) UnmarshalJSON(data []byte) error {
	type alias Searchinfo
	type Searchinfo struct {
		EarliestTime int64 `json:"earliest_time,string"`
		LatestTime   int64 `json:"latest_time,string"`
		*alias
	}
	jsi := Searchinfo{alias: (*alias)(si)}
	if err := json.Unmarshal(data, &jsi); err != nil {
		return fmt.Errorf("%s", err)
	}
	//TODO figure out how to handle subsecond precision
	// It doesn't look like it's passed in :-(
	si.EarliestTime = time.Unix(jsi.EarliestTime, 0)
	si.LatestTime = time.Unix(jsi.LatestTime, 0)
	return nil
}

func NewProcessor(cmd Command, in io.Reader, out io.Writer) *Processor {
	return &Processor{
		Command: cmd,
		input:   bufio.NewReader(in),
		output:  out,
	}
}

func (sp *Processor) Run() error {
	err := sp.run()
	if err != nil {
		sp.Send(map[string]interface{}{
			"inspector": map[string]interface{}{
				"messages": [][]string{
					[]string{"ERROR", "Error " + err.Error()},
				},
			},
		}, nil)
	}
	return err
}
func (sp *Processor) run() error {
	header, _, err := sp.input.ReadLine()
	if err != nil {
		return errors.F(err, "reading header")
	}

	//TODO? verify length
	params := bytes.SplitN(header, []byte{','}, 3)
	if string(params[0]) != "chunked 1.0" {
		return fmt.Errorf("unsupported transport encoding: %q", params[0])
	}

	mdLen, err := strconv.Atoi(string(params[1]))
	if err != nil {
		return errors.F(err, "decoding metadata length")
	}
	buf := make([]byte, mdLen)
	if _, err := sp.input.Read(buf); err != nil {
		return errors.F(err, "reading metadata")
	}

	if err := json.Unmarshal(buf, &sp.Metadata); err != nil {
		return errors.F(err, "decoding metadata")
	}

	//TODO handle body. currently discarding it
	bodyLen, err := strconv.Atoi(string(params[2]))
	if err != nil {
		return errors.F(err, "decoding body length")
	}
	if _, err := io.CopyN(ioutil.Discard, sp.input, int64(bodyLen)); err != nil {
		return errors.F(err, "reading body")
	}

	switch sp.Metadata.Action {
	case "getinfo":
		return sp.Getinfo()
	case "execute":
		return sp.Execute()
	default:
		return fmt.Errorf("unsupported action '%s'", sp.Metadata.Action)
	}
}

func (sp *Processor) Getinfo() error {
	info := sp.Command.Getinfo()
	if info == nil {
		info = map[string]interface{}{}
	}

	if _, ok := info["type"]; !ok {
		info["type"] = "streaming"
	}

	if err := sp.Send(info, nil); err != nil {
		return errors.F(err, "sending getinfo response")
	}

	return sp.Execute()
}

func (sp *Processor) Execute() error {
	chunker, err := sp.Command.Execute(sp.Metadata.Searchinfo)
	if err != nil {
		return errors.F(err, "executing command")
	}
	defer chunker.Close()

	buf := bytes.NewBuffer(nil)
	bufCsv := csv.NewWriter(buf)
	bufCsv.UseCRLF = true
	for {
		cols, data, err := chunker.NextChunk()
		if err != nil {
			return errors.F(err, "retrieving next chunk")
		}
		if cols == nil {
			break
		}
		if len(cols) == 0 {
			continue
		}

		buf.Reset()
		bufCsv.Write(cols)

		rowStrs := make([]string, len(cols))
		for _, row := range data {
			for i, field := range row {
				rowStrs[i] = toString(field)
			}
			bufCsv.Write(rowStrs)
		}

		bufCsv.Flush()

		if err := sp.Send(nil, buf.Bytes()); err != nil {
			return errors.F(err, "sending chunk")
		}
	}

	return errors.F(sp.Send(map[string]interface{}{"finished": true}, nil), "sending finish")
}

func toString(v interface{}) string {
	switch v := v.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	default:
		if v, ok := v.(fmt.Stringer); ok {
			return v.String()
		} else {
			return fmt.Sprintf("%v", v)
		}
	}
}

func (sp *Processor) Send(meta map[string]interface{}, body []byte) error {
	if meta == nil {
		meta = map[string]interface{}{}
	}
	if fin, ok := meta["finished"]; !ok || fin == false {
		meta["partial"] = true
		meta["finished"] = false
	}

	var metaBuf []byte
	if meta != nil {
		var err error
		metaBuf, err = json.Marshal(meta)
		if err != nil {
			return errors.F(err, "encoding metadata")
		}
	}

	if _, err := fmt.Fprintf(sp.output, "chunked 1.0,%d,%d\n", len(metaBuf), len(body)); err != nil {
		return errors.F(err, "writing header")
	}
	if _, err := sp.output.Write(metaBuf); err != nil {
		return errors.F(err, "writing metadata")
	}
	if _, err := sp.output.Write(body); err != nil {
		return errors.F(err, "writing body")
	}
	return nil
}
