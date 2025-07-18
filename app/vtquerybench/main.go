package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

func main() {
	b, err := os.ReadFile("./app/vtquerybench/time_tid.txt")
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(b), "\n")
	queryData := make([][2]string, len(lines))
	for i, line := range lines {
		d := strings.Split(line, ",")
		t, err := time.Parse(time.RFC3339, d[0])
		if err != nil {
			panic(err)
		}
		queryData[i][0] = strconv.FormatInt(t.UnixNano(), 10)
		queryData[i][1] = d[1]
	}

	rand.Shuffle(len(queryData), func(i, j int) {
		queryData[i], queryData[j] = queryData[j], queryData[i]
	})

	benchmarkQuery(queryData[:5])

}

var (
	vtTraceEndpoint         = "http://%s:10428/select/jaeger/api/traces/%s"
	clickhouseTraceEndpoint = "http://%s:16686/api/traces/%s"
	tempoTraceEndpoint      = "http://%s:3200/api/v2/traces/%s"

	//victoriaTracesHost          = "35.236.177.218"
	//victoriaTracesOptimizedHost = "35.194.162.46"
	//jaegerClickHouseHost        = "34.81.255.58"
	//tempoHost                   = "104.199.134.199"

	victoriaTracesHost          = "10.140.15.196"
	victoriaTracesOptimizedHost = "10.140.15.200"
	jaegerClickHouseHost        = "10.140.15.205"
	tempoHost                   = "10.140.15.204"
)

func benchmarkQuery(queryData [][2]string) {
	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		for i := range queryData {
			t1 := time.Now()
			resp, err := http.Get(fmt.Sprintf(vtTraceEndpoint, victoriaTracesHost, queryData[i][1]))
			if err != nil || resp.StatusCode != 200 {
				logger.Errorf("cannot get from victoriaTraces, %s, err %s or status != 200.", queryData[i][0], err)
			}
			logger.Infof("vt: timestamp: %s, duration: %dns", queryData[i][0], time.Since(t1).Nanoseconds())
		}
		wg.Done()
	}()

	go func() {
		for i := range queryData {
			t2 := time.Now()
			resp, err := http.Get(fmt.Sprintf(vtTraceEndpoint, victoriaTracesOptimizedHost, queryData[i][1]))
			if err != nil || resp.StatusCode != 200 {
				logger.Errorf("cannot get from victoriaTraces opt, %s, err %s or status != 200.", queryData[i][0], err)
			}
			logger.Infof("vt opt: timestamp: %s, duration: %dns", queryData[i][0], time.Since(t2).Nanoseconds())
		}
		wg.Done()
	}()

	go func() {
		for i := range queryData {
			t3 := time.Now()
			resp, err := http.Get(fmt.Sprintf(clickhouseTraceEndpoint, jaegerClickHouseHost, queryData[i][1]))
			if err != nil || resp.StatusCode != 200 {
				logger.Errorf("cannot get from ch, %s, err %s or status != 200.", queryData[i][0], err)
			}
			logger.Infof("ch: timestamp: %s, duration: %dns", queryData[i][0], time.Since(t3).Nanoseconds())
		}
		wg.Done()
	}()

	go func() {
		for i := range queryData {
			t4 := time.Now()
			resp, err := http.Get(fmt.Sprintf(tempoTraceEndpoint, tempoHost, queryData[i][1]))
			if err != nil || resp.StatusCode != 200 {
				logger.Errorf("cannot get from tempo, %s, err %s or status != 200.", queryData[i][0], err)
			}
			logger.Infof("tempo: timestamp: %s, duration: %dns", queryData[i][0], time.Since(t4).Nanoseconds())
		}
		wg.Done()
	}()
	wg.Wait()
}
