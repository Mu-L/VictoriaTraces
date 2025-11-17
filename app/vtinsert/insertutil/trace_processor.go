package insertutil

import (
	"flag"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/cespare/xxhash/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/metrics"

	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

var (
	traceMaxDuration = flag.Duration("insert.traceMaxDuration", time.Minute, "Maximum duration for a trace. VictoriaTraces creates an index for each trace ID based on its start and end times."+
		"Each trace ID must wait in the queue for -insert.traceMaxDuration, continuously updating its start and end times before being inserted into the index.")
)

// traceSpanProcessor is a wrapper logMessageProcessor.
type traceSpanProcessor struct {
	lmp *logMessageProcessor
}

// NewTraceProcessor returns new TraceSpansProcessor for the given cp.
//
// MustClose() must be called on the returned TraceSpansProcessor when it is no longer needed.
func (cp *CommonParams) NewTraceProcessor(protocolName string, isStreamMode bool) LogMessageProcessor {
	lr := logstorage.GetLogRows(cp.StreamFields, cp.IgnoreFields, cp.DecolorizeFields, cp.ExtraFields, *defaultMsgValue)
	rowsIngestedTotal := metrics.GetOrCreateCounter(fmt.Sprintf("vt_rows_ingested_total{type=%q}", protocolName))
	bytesIngestedTotal := metrics.GetOrCreateCounter(fmt.Sprintf("vt_bytes_ingested_total{type=%q}", protocolName))
	flushDuration := metrics.GetOrCreateSummary(fmt.Sprintf("vt_insert_flush_duration_seconds{type=%q}", protocolName))
	tsp := &traceSpanProcessor{
		lmp: &logMessageProcessor{
			cp: cp,
			lr: lr,

			rowsIngestedTotal:  rowsIngestedTotal,
			bytesIngestedTotal: bytesIngestedTotal,
			flushDuration:      flushDuration,

			stopCh: make(chan struct{}),
		},
	}

	if isStreamMode {
		tsp.lmp.initPeriodicFlush()
	}

	messageProcessorCount.Add(1)
	return tsp
}

// The following methods are for external data ingestion. They could be called from OTLP handlers.

// AddRow adds new log message to lmp with the given timestamp and fields.
// It also creates index if the current process is a storage node (VictoriaTraces Single-node).
//
// If streamFields is non-nil, then it is used as log stream fields instead of the pre-configured stream fields.
func (tsp *traceSpanProcessor) AddRow(timestamp int64, fields, streamFields []logstorage.Field) {
	if logRowsStorage.IsLocalStorage() {
		tsp.pushTraceToIndexQueue(tsp.lmp.cp.TenantID, fields)
	}
	tsp.lmp.AddRow(timestamp, fields, streamFields)
}

func (tsp *traceSpanProcessor) MustClose() {
	tsp.lmp.MustClose()
}

type indexEntry struct {
	tenantID  logstorage.TenantID
	traceID   string
	startTime string
	endTime   string
}

type indexTimer struct {
	traceID string
	addTime uint64
}

var (
	// traceIDCh holds traceID and the time this traceID is added to the channel.
	traceIDCh = make(chan indexTimer, 10000)
	// traceIDIndexMap holds the index data of a traceID before this index could be persisted.
	traceIDIndexMap = make(map[string]*indexEntry)
	// mu protects traceIDIndexMap
	mu = sync.Mutex{}
	// logMessageProcessorMap holds lmp for different tenants.
	logMessageProcessorMap = make(map[logstorage.TenantID]LogMessageProcessor)

	stopCh = make(chan struct{})
)

// pushTraceToIndexQueue is for trace from LogMessageProcessor. It adds trace ID, startTime, endTime of the span to the FIFO queue.
// Each item in the queue will be popped after certain interval, and carries the min(startTime), max(endTime) of this trace ID.
func (tsp *traceSpanProcessor) pushTraceToIndexQueue(tenant logstorage.TenantID, fields []logstorage.Field) bool {
	var traceID, startTime, endTime string

	i := len(fields) - 1
	// find trace ID in revert order.
	for ; i >= 0; i-- {
		if fields[i].Name == otelpb.TraceIDField {
			traceID = strings.Clone(fields[i].Value)
			break
		}
	}

	if traceID == "" {
		return false
	}

	// find endTime of the span in revert order, it should be right before trace ID field.
	for ; i >= 0; i-- {
		if fields[i].Name == otelpb.EndTimeUnixNanoField {
			endTime = strings.Clone(fields[i].Value)
			break
		}
	}

	// find startTime of the span in revert order, it should be right before endTime field.
	for ; i >= 0; i-- {
		if fields[i].Name == otelpb.StartTimeUnixNanoField {
			startTime = strings.Clone(fields[i].Value)
			break
		}
	}

	// to secure an index entry will be created even if the span does not have startTime and endTime.
	if startTime == "" {
		startTime = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	if endTime == "" {
		endTime = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return pushIndexToQueue(tenant, traceID, startTime, endTime)
}

// The following methods are for native data ingestion protocol. They must be called from `internalinsert`.

// AddInsertRow is a wrapper function of logMessageProcessor.AddInsertRow.
// while processing trace spans as logs, traceSpanProcessor also create trace ID index for each new trace ID.
func (tsp *traceSpanProcessor) AddInsertRow(r *logstorage.InsertRow) {
	// create index <traceID, startTime, endTime> if the current process is a storage node (VictoriaTraces single-node, or vtstorage).
	if logRowsStorage.IsLocalStorage() && !tsp.pushNativeRowToIndexQueue(r) {
		return
	}
	tsp.lmp.AddInsertRow(r)
}

// pushNativeRowToIndexQueue is for native data ingestion protocol. It adds trace ID, startTime, endTime of the span to the FIFO queue.
// Each item in the queue will be popped after certain interval, and carries the min(startTime), max(endTime) of this trace ID.
func (tsp *traceSpanProcessor) pushNativeRowToIndexQueue(r *logstorage.InsertRow) bool {
	var traceID, startTime, endTime string

	i := len(r.Fields) - 1
	// find trace ID in revert order.
	for ; i >= 0; i-- {
		if r.Fields[i].Name == otelpb.TraceIDField {
			traceID = strings.Clone(r.Fields[i].Value)
			break
		}
	}

	if traceID == "" {
		return false
	}

	// find endTime of the span in revert order, it should be right before trace ID field.
	for ; i >= 0; i-- {
		if r.Fields[i].Name == otelpb.EndTimeUnixNanoField {
			endTime = strings.Clone(r.Fields[i].Value)
			break
		}
	}

	// find startTime of the span in revert order, it should be right before endTime field.
	for ; i >= 0; i-- {
		if r.Fields[i].Name == otelpb.StartTimeUnixNanoField {
			startTime = strings.Clone(r.Fields[i].Value)
			break
		}
	}

	// to secure an index entry will be created even if the span does not have startTime and endTime.
	if startTime == "" {
		startTime = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	if endTime == "" {
		endTime = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return pushIndexToQueue(r.TenantID, traceID, startTime, endTime)
}

// The following functions are shared by external data ingestion protocol (from LogMessageProcessor interface),
// or native data ingestion protocol (from InsertRowProcessor interface).

// pushIndexToQueue organize index data (from LogMessageProcessor interface or InsertRowProcessor interface)
// and push it to the queue.
func pushIndexToQueue(tenantID logstorage.TenantID, traceID string, startTime, endTime string) bool {
	mu.Lock()
	defer mu.Unlock()

	index, ok := traceIDIndexMap[traceID]
	if ok {
		index.startTime = min(index.startTime, startTime)
		index.endTime = max(index.endTime, endTime)
		return true
	}

	index = &indexEntry{
		tenantID:  tenantID,
		traceID:   traceID,
		startTime: startTime,
		endTime:   endTime,
	}

	traceIDIndexMap[traceID] = index
	traceIDCh <- indexTimer{
		traceID: traceID,
		addTime: fasttime.UnixTimestamp(),
	}
	return true
}

// MustStartIndexWorker starts a single goroutine worker that reads from traceIDCh and write the index entry to storage.
func MustStartIndexWorker() {
	go func() {
		for {
			select {
			case <-stopCh:
				// todo finish traceIDCh before exit
				return
			case index := <-traceIDCh:
				waitMs := (*traceMaxDuration).Milliseconds() - int64((fasttime.UnixTimestamp()-index.addTime)*1000)
				if waitMs > 0 {
					time.Sleep(time.Duration(waitMs) * time.Millisecond)
				}

				mu.Lock()
				indexData := traceIDIndexMap[index.traceID]
				delete(traceIDIndexMap, index.traceID)
				mu.Unlock()

				// There's no need to use mutex because this worker is single goroutine.
				lmp, ok := logMessageProcessorMap[indexData.tenantID]
				if !ok {
					// init the lmp for the current tenant
					cp := CommonParams{
						TenantID:   indexData.tenantID,
						TimeFields: []string{"_time"},
					}
					lmp = cp.NewLogMessageProcessor("internalinsert_index", true)
					logMessageProcessorMap[indexData.tenantID] = lmp
				}

				lmp.AddRow(int64(index.addTime)*1000000000,
					// fields
					[]logstorage.Field{
						{Name: "_msg", Value: "-"},
						{Name: otelpb.TraceIDIndexFieldName, Value: indexData.traceID},
						{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: indexData.startTime},
						{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: indexData.endTime},
					},
					// stream fields
					[]logstorage.Field{
						{Name: otelpb.TraceIDIndexStreamName, Value: strconv.FormatUint(xxhash.Sum64String(indexData.traceID)%otelpb.TraceIDIndexPartitionCount, 10)},
					},
				)
			}
		}
	}()
}

func MustStopIndexWorker() {
	close(traceIDCh)
	close(stopCh)
	for _, lmp := range logMessageProcessorMap {
		lmp.MustClose()
	}
}
