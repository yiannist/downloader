// Package stats provides stats reporting and storing abstractions.
package stats

import (
	"context"
	"expvar"
	"log"
	"time"
)

// Stats is the statistics reporting entity of the downloader.
// It acts as a deamon and reports stats using
// the provided reportfunc on the specified interval.
type Stats struct {
	*expvar.Map
	interval   time.Duration
	reportfunc func(m *expvar.Map)
}

// Reporter encapsulates an expvar Map and acts as a metric reporting interface for each module
//var Reporter *Stats

// Run calls the report function of Stats using the specified interval/
// It shuts down when the provided context is cancelled
func (s *Stats) Run(ctx context.Context) {
	tick := time.NewTicker(s.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("Stats Deamon Exiting")
			return
		case <-tick.C:
			s.reportfunc(s.Map)
		}
	}
}

// New initializes the Reporter and start Run
func New(id string, interval time.Duration, report func(*expvar.Map)) *Stats {
	var statsMap *expvar.Map
	if val := expvar.Get(id); val != nil {
		if newmap, ok := val.(*expvar.Map); ok {
			log.Printf("Stats for %s reinitialized!", id)
			statsMap = newmap
			statsMap.Init()
		}
	}
	if statsMap == nil {
		statsMap = expvar.NewMap(id)
	}

	// serve as liveness indicator for modules registering metrics
	statsMap.Add("alive", 1)

	return &Stats{statsMap, interval, report}
}
