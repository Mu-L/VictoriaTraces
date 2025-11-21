package insertutil

import (
	"flag"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/cespare/xxhash/v2"

	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

var (
	traceMaxDuration = flag.Duration("insert.traceMaxDuration", time.Minute, "Maximum duration for a trace. VictoriaTraces creates an index for each trace ID based on its start and end times."+
		"Each trace ID must wait in the queue for -insert.traceMaxDuration, continuously updating its start and end times before being inserted into the index.")
)

type indexEntry struct {
	tenantID      logstorage.TenantID
	startTimeNano string
	endTimeNano   string
}

var (
	// traceIDIndexMapCur and traceIDIndexMapPrev holds the index data *indexEntry for each traceID, before they could be persisted.
	// it mainly tracks the start time and end time of a trace, which could be edited before they're persisted.
	//
	// - The cur map can accept new traceID and *indexEntry.
	// - The prev map only serves for fast lookup of existing *indexEntry.
	traceIDIndexMapCur  = &sync.Map{}
	traceIDIndexMapPrev = &sync.Map{}

	// logMessageProcessorMap holds lmp for different tenants.
	logMessageProcessorMap = make(map[logstorage.TenantID]LogMessageProcessor)

	// indexWorkerWg is the WaitGroup for IndexWorker. indexWorkerWg.Wait() should be used during shutdown.
	indexWorkerWg = sync.WaitGroup{}
	stopCh        = make(chan struct{})
)

// pushIndexToQueue organize index data (from LogMessageProcessor interface or InsertRowProcessor interface)
// and push it to the queue.
func pushIndexToQueue(tenantID logstorage.TenantID, traceID string, startTime, endTime string) bool {
	select {
	case <-stopCh:
		// during stop, no data should be pushed to the queue anymore.
		return false
	default:
		index, ok := traceIDIndexMapCur.Load(traceID)
		if ok {
			idxEntry := index.(*indexEntry)
			idxEntry.startTimeNano = min(idxEntry.startTimeNano, startTime)
			idxEntry.endTimeNano = max(idxEntry.endTimeNano, endTime)
			return true
		}

		index, ok = traceIDIndexMapPrev.Load(traceID)
		if ok {
			idxEntry := index.(*indexEntry)
			idxEntry.startTimeNano = min(idxEntry.startTimeNano, startTime)
			idxEntry.endTimeNano = max(idxEntry.endTimeNano, endTime)
			return true
		}

		idxEntry := &indexEntry{
			tenantID:      tenantID,
			startTimeNano: startTime,
			endTimeNano:   endTime,
		}

		traceIDIndexMapCur.Store(traceID, idxEntry)
	}

	return true
}

// MustStartIndexWorker starts a single goroutine worker that reads from traceIDCh and write the index entry to storage.
func MustStartIndexWorker() {
	indexWorkerWg.Add(1)
	go func() {
		defer indexWorkerWg.Done()

		ticker := time.NewTicker(*traceMaxDuration / 2)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				// persist all the index in the queue,
				// even though they're still fresh (haven't waited for *traceMaxDuration).
				traceIDIndexMapPrev.Range(writeIndexInMap)
				traceIDIndexMapCur.Range(writeIndexInMap)

				return
			case <-ticker.C:
				traceIDIndexMapPrev.Range(writeIndexInMap)

				traceIDIndexMapPrev.Clear()
				traceIDIndexMapCur, traceIDIndexMapPrev = traceIDIndexMapPrev, traceIDIndexMapCur
			}
		}
	}()
}

// writeIndexInMap transform the
func writeIndexInMap(traceID, index any) bool {
	idxEntry := index.(*indexEntry)
	lmp, ok := logMessageProcessorMap[idxEntry.tenantID]
	if !ok {
		// init the lmp for the current tenant
		cp := CommonParams{
			TenantID:   idxEntry.tenantID,
			TimeFields: []string{"_time"},
		}
		lmp = cp.NewLogMessageProcessor("internalinsert_index", true)

		// only current goroutine can read/write this map, so mutex is not needed.
		// consider adding a mutex if index worker is scaled to multi-goroutines.
		logMessageProcessorMap[idxEntry.tenantID] = lmp
	}

	indexTimestamp, err := strconv.ParseInt(idxEntry.startTimeNano, 10, 64)
	if err != nil {
		logger.Errorf("trace index: cannot parse start time %q to int64", idxEntry.startTimeNano)
		return true
	}
	lmp.AddRow(indexTimestamp,
		// fields
		[]logstorage.Field{
			{Name: "_msg", Value: "-"},
			{Name: otelpb.TraceIDIndexFieldName, Value: traceID.(string)},
			{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: idxEntry.startTimeNano},
			{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: idxEntry.endTimeNano},
		},
		// stream fields
		[]logstorage.Field{
			{Name: otelpb.TraceIDIndexStreamName, Value: strconv.FormatUint(xxhash.Sum64String(traceID.(string))%otelpb.TraceIDIndexPartitionCount, 10)},
		},
	)
	return true
}

func MustStopIndexWorker() {
	close(stopCh)

	// wait until all the index workers exit
	indexWorkerWg.Wait()

	for _, lmp := range logMessageProcessorMap {
		lmp.MustClose()
	}
}
