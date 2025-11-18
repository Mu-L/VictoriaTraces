package insertutil

import (
	"flag"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
	"github.com/cespare/xxhash/v2"
	"strconv"
	"sync"
	"time"
)

var (
	traceMaxDuration = flag.Duration("insert.traceMaxDuration", time.Minute, "Maximum duration for a trace. VictoriaTraces creates an index for each trace ID based on its start and end times."+
		"Each trace ID must wait in the queue for -insert.traceMaxDuration, continuously updating its start and end times before being inserted into the index.")
)

type indexEntry struct {
	tenantID      logstorage.TenantID
	startTimeNano string
	endTimeNano   string
	addTime       uint64
}

var (
	// traceIDIndexMapCur and traceIDIndexMapPrev holds the index data of a traceID before this index could be persisted.
	// The cur map can accept new entries.
	traceIDIndexMapCur = make(map[string]*indexEntry)
	// The prev map only serves for fast lookup of existing entries. Write operation can be performed on the *indexEntry,
	// but not on the prev map.
	traceIDIndexMapPrev = make(map[string]*indexEntry)

	// mu protects traceIDIndexMapCur
	mu = sync.RWMutex{}
	// muSwitch locks the read and write for both cur and prev
	muSwitch = sync.Mutex{}

	// logMessageProcessorMap holds lmp for different tenants.
	logMessageProcessorMap = make(map[logstorage.TenantID]LogMessageProcessor)

	stopCh = make(chan struct{})
)

// pushIndexToQueue organize index data (from LogMessageProcessor interface or InsertRowProcessor interface)
// and push it to the queue.
func pushIndexToQueue(tenantID logstorage.TenantID, traceID string, startTime, endTime string) bool {
	mu.RLock()

	index, ok := traceIDIndexMapCur[traceID]
	if ok {
		index.startTimeNano = min(index.startTimeNano, startTime)
		index.endTimeNano = max(index.endTimeNano, endTime)
		mu.RUnlock()
		return true
	}

	index, ok = traceIDIndexMapPrev[traceID]
	if ok {
		index.startTimeNano = min(index.startTimeNano, startTime)
		index.endTimeNano = max(index.endTimeNano, endTime)
		mu.RUnlock()
		return true
	}

	mu.RUnlock()

	index = &indexEntry{
		tenantID:      tenantID,
		startTimeNano: startTime,
		endTimeNano:   endTime,
		addTime:       fasttime.UnixTimestamp(),
	}

	mu.Lock()
	defer mu.Unlock()
	traceIDIndexMapCur[traceID] = index
	return true
}

// MustStartIndexWorker starts a single goroutine worker that reads from traceIDCh and write the index entry to storage.
func MustStartIndexWorker() {
	go func() {
		ticker := time.NewTicker(*traceMaxDuration / 2)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				// todo finish traceIDCh before exit
				return
			case <-ticker.C:
				mu.Lock()
				for traceID, idxEntry := range traceIDIndexMapPrev {
					lmp, ok := logMessageProcessorMap[idxEntry.tenantID]
					if !ok {
						// init the lmp for the current tenant
						cp := CommonParams{
							TenantID:   idxEntry.tenantID,
							TimeFields: []string{"_time"},
						}
						lmp = cp.NewLogMessageProcessor("internalinsert_index", true)
						logMessageProcessorMap[idxEntry.tenantID] = lmp
					}

					lmp.AddRow(int64(idxEntry.addTime)*1000000000,
						// fields
						[]logstorage.Field{
							{Name: "_msg", Value: "-"},
							{Name: otelpb.TraceIDIndexFieldName, Value: traceID},
							{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: idxEntry.startTimeNano},
							{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: idxEntry.endTimeNano},
						},
						// stream fields
						[]logstorage.Field{
							{Name: otelpb.TraceIDIndexStreamName, Value: strconv.FormatUint(xxhash.Sum64String(traceID)%otelpb.TraceIDIndexPartitionCount, 10)},
						},
					)
				}

				clear(traceIDIndexMapPrev)
				traceIDIndexMapCur, traceIDIndexMapPrev = traceIDIndexMapPrev, traceIDIndexMapCur
				mu.Unlock()
			}
		}
	}()
}

func MustStopIndexWorker() {
	close(stopCh)
	for _, lmp := range logMessageProcessorMap {
		lmp.MustClose()
	}
}
