// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"encoding/binary"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"hash/fnv"
	"os"
	"unsafe"
)

// StopDuplicateRemover merges semantically equivalent stops
type StopDuplicateRemover struct {
}

// Run this StopDuplicateRemover on some feed
func (sdr StopDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant stops... ")
	bef := len(feed.Stops)

	// run two times to catch parent equivalencies
	for i := 0; i < 3; i++ {
		stoptimes := make(map[*gtfs.Stop][]*gtfs.StopTime, len(feed.Stops))
		stops := make(map[*gtfs.Stop][]*gtfs.Stop, len(feed.Stops))
		transfers := make(map[*gtfs.Stop][]*gtfs.Transfer, len(feed.Stops))
		pathways := make(map[*gtfs.Stop][]*gtfs.Pathway, len(feed.Stops))
		proced := make(map[*gtfs.Stop]bool, len(feed.Stops))

		chunks := sdr.getStopChunks(feed)

		// collect stop times that use stops
		for _, t := range feed.Trips {
			for i, st := range t.StopTimes {
				stoptimes[st.Stop] = append(stoptimes[st.Stop], &t.StopTimes[i])
			}
		}

		// collect stops that use stops as parents
		for _, s := range feed.Stops {
			if s.Parent_station != nil {
				stops[s.Parent_station] = append(stops[s.Parent_station], s)
			}
		}

		// collect transfers that use stop
		for _, t := range feed.Transfers {
			transfers[t.From_stop] = append(transfers[t.From_stop], t)
			if t.From_stop != t.To_stop {
				transfers[t.To_stop] = append(transfers[t.To_stop], t)
			}
		}

		// collect pathways that use stop
		for _, p := range feed.Pathways {
			pathways[p.From_stop] = append(pathways[p.From_stop], p)
			if p.From_stop != p.To_stop {
				pathways[p.To_stop] = append(pathways[p.To_stop], p)
			}
		}

		for _, s := range feed.Stops {
			if _, ok := proced[s]; ok {
				continue
			}

			hash := sdr.stopHash(s)
			eqStops := sdr.getEquivalentStops(s, feed, chunks[hash])

			if len(eqStops) > 0 {
				sdr.combineStops(feed, append(eqStops, s), stoptimes, stops, transfers, pathways)

				for _, s := range eqStops {
					proced[s] = true
				}

				proced[s] = true
			}
		}
	}

	fmt.Fprintf(os.Stdout, "done. (-%d stops [-%.2f%%])\n", (bef - len(feed.Stops)), 100.0*float64(bef-len(feed.Stops))/float64(bef))
}

// Returns the feed's stops that are equivalent to stop
func (sdr StopDuplicateRemover) getEquivalentStops(stop *gtfs.Stop, feed *gtfsparser.Feed, chunks [][]*gtfs.Stop) []*gtfs.Stop {
	rets := make([][]*gtfs.Stop, len(chunks))
	sem := make(chan empty, len(chunks))

	for i, c := range chunks {
		go func(j int, chunk []*gtfs.Stop) {
			for _, s := range chunk {
				if s == stop {
					continue
				}
				if _, ok := feed.Stops[s.Id]; !ok {
					continue
				}
				if sdr.stopEquals(s, stop) {
					rets[j] = append(rets[j], s)
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	// combine results
	ret := make([]*gtfs.Stop, 0)

	for _, r := range rets {
		ret = append(ret, r...)
	}

	return ret
}

// Combine a slice of equal stops into a single stop
func (sdr StopDuplicateRemover) combineStops(feed *gtfsparser.Feed, stops []*gtfs.Stop, stoptimes map[*gtfs.Stop][]*gtfs.StopTime, pstops map[*gtfs.Stop][]*gtfs.Stop,
	transfers map[*gtfs.Stop][]*gtfs.Transfer,
	pathways map[*gtfs.Stop][]*gtfs.Pathway) {
	// heuristic: use the stop with the shortest ID as 'reference'
	ref := stops[0]

	for _, s := range stops {
		if len(s.Id) < len(ref.Id) {
			ref = s
		}
	}

	for _, s := range stops {
		if s == ref {
			continue
		}

		for i, st := range stoptimes[s] {
			if st.Stop == s {
				stoptimes[s][i].Stop = ref
				stoptimes[ref] = append(stoptimes[ref], stoptimes[s][i])
			}
		}

		for i, ps := range pstops[s] {
			if ps.Parent_station == s {
				pstops[s][i].Parent_station = ref
				pstops[ref] = append(pstops[ref], pstops[s][i])
			}
		}

		for i, t := range transfers[s] {
			up := false
			if t.From_stop == s {
				transfers[s][i].From_stop = ref
				up = true
			}
			if t.To_stop == s {
				transfers[s][i].To_stop = ref
				up = true
			}
			if up {
				transfers[ref] = append(transfers[ref], transfers[s][i])
			}
		}

		for i, p := range pathways[s] {
			up := false
			if p.From_stop == s {
				pathways[s][i].From_stop = ref
				up = true
			}
			if p.To_stop == s {
				pathways[s][i].To_stop = ref
				up = true
			}
			if up {
				pathways[ref] = append(pathways[ref], pathways[s][i])
			}
		}

		delete(feed.Stops, s.Id)
	}
}

func (sdr StopDuplicateRemover) getStopChunks(feed *gtfsparser.Feed) map[uint32][][]*gtfs.Stop {
	numchunks := MaxParallelism()

	// maps stop (parents) to all trips originating from it
	stops := make(map[uint32][]*gtfs.Stop)
	chunks := make(map[uint32][][]*gtfs.Stop)

	for _, r := range feed.Stops {
		hash := sdr.stopHash(r)
		stops[hash] = append(stops[hash], r)
	}

	for hash := range stops {
		chunksize := (len(stops[hash]) + numchunks - 1) / numchunks
		chunks[hash] = make([][]*gtfs.Stop, numchunks)
		curchunk := 0

		for _, t := range stops[hash] {
			chunks[hash][curchunk] = append(chunks[hash][curchunk], t)
			if len(chunks[hash][curchunk]) == chunksize {
				curchunk++
			}
		}
	}

	return chunks
}

func (sdr StopDuplicateRemover) stopHash(s *gtfs.Stop) uint32 {
	h := fnv.New32a()

	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, uint64(uintptr(unsafe.Pointer(s.Parent_station))))
	h.Write(b)

	binary.LittleEndian.PutUint64(b, uint64(uintptr(unsafe.Pointer(s.Level))))
	h.Write(b)

	binary.LittleEndian.PutUint64(b, uint64(s.Location_type))
	h.Write(b)

	binary.LittleEndian.PutUint64(b, uint64(s.Wheelchair_boarding))
	h.Write(b)

	h.Write([]byte(s.Code))
	h.Write([]byte(s.Name))
	h.Write([]byte(s.Desc))
	h.Write([]byte(s.Zone_id))
	h.Write([]byte(s.Timezone.GetTzString()))
	h.Write([]byte(s.Platform_code))

	return h.Sum32()
}

// Check if two stops are equal, distances under 1 m count as equal
func (sdr StopDuplicateRemover) stopEquals(a *gtfs.Stop, b *gtfs.Stop) bool {
	return a.Code == b.Code &&
		a.Name == b.Name &&
		a.Desc == b.Desc &&
		a.Zone_id == b.Zone_id &&
		a.Url == b.Url &&
		a.Location_type == b.Location_type &&
		a.Parent_station == b.Parent_station &&
		a.Timezone == b.Timezone &&
		a.Wheelchair_boarding == b.Wheelchair_boarding &&
		a.Level == b.Level &&
		a.Platform_code == b.Platform_code &&
		distSApprox(a, b) <= 1.0
}
