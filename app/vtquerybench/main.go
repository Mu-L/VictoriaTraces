package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

func main() {
	//b, err := os.ReadFile("./app/vtquerybench/time_tid.txt")
	//if err != nil {
	//	panic(err)
	//}
	//lines := strings.Split(string(b), "\n")
	//queryData := make([][2]string, len(lines))
	//for i, line := range lines {
	//	d := strings.Split(line, ",")
	//	t, err := time.Parse(time.RFC3339, d[0])
	//	if err != nil {
	//		panic(err)
	//	}
	//	queryData[i][0] = strconv.FormatInt(t.UnixNano(), 10)
	//	queryData[i][1] = d[1]
	//}
	//
	//rand.Shuffle(len(queryData), func(i, j int) {
	//	queryData[i], queryData[j] = queryData[j], queryData[i]
	//})

	//benchmarkQuery(queryData[:5])

	benchSearch()
}

var (
	vtTraceEndpoint         = "http://%s:10428/select/jaeger/api/traces/%s"
	clickhouseTraceEndpoint = "http://%s:16686/api/traces/%s"
	tempoTraceEndpoint      = "http://%s:3200/api/v2/traces/%s"

	vtTraceSearchEndpoint         = "http://%s:10428/select/jaeger/api/traces?%s"
	clickhouseTraceSearchEndpoint = "http://%s:16686/api/traces?%s"

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
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
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
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
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
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
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
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
		}
		wg.Done()
	}()
	wg.Wait()
}

var (
	searchFilter = []map[string]string{
		{
			"service":     "checkout",
			"tags":        `{"app.order.id":"9fed9dd1-5e60-11f0-9fd3-0242ac130015","internal.span.format":"otlp"}`,
			"minDuration": "5s",
			"maxDuration": "12s",
			"limit":       "100",
		},
		{
			"service":     "checkout",
			"tags":        `{"app.order.id":"9fed9dd1-5e60-11f0-9fd3-0242ac130015"}`,
			"minDuration": "5s",
			"maxDuration": "12s",
			"limit":       "50",
		},
		{
			"service":     "checkout",
			"tags":        `{"internal.span.format":"otlp"}`,
			"minDuration": "5s",
			"maxDuration": "12s",
			"limit":       "50",
		},
		{
			"service":     "checkout",
			"tags":        `{"internal.span.format":"otlp"}`,
			"minDuration": "5s",
			"maxDuration": "12s",
			"limit":       "100",
		},
		{
			"service":     "checkout",
			"tags":        `{"rpc.system":"grpc","rpc.service":"oteldemo.ProductCatalogService"}`,
			"minDuration": "0s",
			"maxDuration": "2s",
			"limit":       "100",
		},
		{
			"service":     "checkout",
			"tags":        `{"rpc.system":"grpc"}`,
			"minDuration": "0s",
			"maxDuration": "2s",
			"limit":       "50",
		},
		{
			"service":     "checkout",
			"tags":        `{"rpc.service":"oteldemo.ProductCatalogService"}`,
			"minDuration": "0s",
			"maxDuration": "2s",
			"limit":       "100",
		},
		{
			"service":     "checkout",
			"tags":        `{"rpc.grpc.status_code":"0","rpc.method":"GetQuote"}`,
			"minDuration": "0s",
			"maxDuration": "1s",
			"limit":       "100",
		},
		{
			"service":     "checkout",
			"tags":        `{"rpc.grpc.status_code":"0"}`,
			"minDuration": "0s",
			"maxDuration": "1s",
			"limit":       "50",
		},
		{
			"service":     "checkout",
			"tags":        `{"rpc.method":"GetQuote"}`,
			"minDuration": "0s",
			"maxDuration": "1s",
			"limit":       "50",
		},
		{
			"service":     "payment",
			"tags":        `{"app.payment.amount":"418.75","rpc.method":"Charge"}`,
			"minDuration": "0s",
			"maxDuration": "1s",
			"limit":       "100",
		},
		{
			"service":     "payment",
			"tags":        `{"rpc.method":"Charge"}`,
			"minDuration": "0s",
			"maxDuration": "1s",
			"limit":       "100",
		},
		{
			"service":     "payment",
			"tags":        `{"app.payment.amount":"418.75"}`,
			"minDuration": "0s",
			"maxDuration": "1s",
			"limit":       "100",
		},
		{
			"service":     "cart",
			"tags":        `{"server.address":"valkey-cart"}`,
			"minDuration": "0s",
			"maxDuration": "50ms",
			"limit":       "100",
		},
		{
			"serviceName": "product-catalog",
			"tags":        `{"app.product.id":"66VCHSJNUP"}`,
			"minDuration": "0s",
			"maxDuration": "50ms",
			"limit":       "100",
		},
	}
)

func benchSearch() {
	// generate request list
	requestList := make([]map[string]string, 0, 100*len(searchFilter))
	for i := 0; i < 100; i++ {
		requestList = append(requestList, searchFilter...)
	}
	rand.Shuffle(len(requestList), func(i, j int) {
		requestList[i], requestList[j] = requestList[j], requestList[i]
	})

	// generate request time range list
	minTimestamp, maxTimestamp := 1752725963, 1752807421
	timeRangeList := make([][2]int, 0, 100*len(searchFilter))
	for i := 0; i < 100*len(searchFilter); i++ {
		startTime := minTimestamp + rand.Intn(maxTimestamp-minTimestamp)
		endTime := startTime + 600 + rand.Intn(3600) // 10min - hour
		timeRangeList = append(timeRangeList, [2]int{startTime * 1000000, endTime * 1000000})
	}

	if len(requestList) != len(timeRangeList) {
		panic("incorrect number of params")
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := range requestList {
			u := url.Values{}
			for k, v := range requestList[i] {
				u.Set(k, v)
			}
			u.Set("start", strconv.Itoa(timeRangeList[i][0]))
			u.Set("end", strconv.Itoa(timeRangeList[i][1]))
			target := fmt.Sprintf(vtTraceSearchEndpoint, victoriaTracesOptimizedHost, u.Encode())
			t1 := time.Now()
			resp, err := http.Get(target)
			if err != nil || resp.StatusCode != 200 {
				logger.Errorf("cannot get from victoriaTraces, %s, err %s or status != 200.", target, err)
			}
			logger.Infof("vt opt: duration: %dns", time.Since(t1).Nanoseconds())
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
		}
		wg.Done()
	}()

	go func() {
		for i := range requestList {
			u := url.Values{}
			for k, v := range requestList[i] {
				u.Set(k, v)
			}
			u.Set("start", strconv.Itoa(timeRangeList[i][0]))
			u.Set("end", strconv.Itoa(timeRangeList[i][1]))
			target := fmt.Sprintf(clickhouseTraceSearchEndpoint, jaegerClickHouseHost, u.Encode())
			t1 := time.Now()
			resp, err := http.Get(target)
			if err != nil || resp.StatusCode != 200 {
				logger.Errorf("cannot get from clickhouse, %s, err %s or status != 200.", target, err)
			}
			logger.Infof("clickhouse: duration: %dns", time.Since(t1).Nanoseconds())
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
		}
		wg.Done()
	}()
	wg.Wait()
}
