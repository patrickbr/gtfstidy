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
	DistThresholdStop    float64
	DistThresholdStation float64
	Fuzzy                bool
}

// Run this StopDuplicateRemover on some feed
func (sdr StopDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant stops... ")
	bef := len(feed.Stops)

	levels := make(map[*gtfs.Level][]*gtfs.Stop, len(feed.Levels))
	procedLvls := make(map[*gtfs.Level]bool, len(feed.Levels))

	// collect levels that use stops as parents
	for _, s := range feed.Stops {
		if s.Level != nil {
			levels[s.Level] = append(levels[s.Level], s)
		}
	}

	// first, remove level duplicates
	for _, l := range feed.Levels {
		if _, ok := procedLvls[l]; ok {
			continue
		}

		eqLevels := sdr.getEquivalentLevels(l, feed)

		if len(eqLevels) > 0 {
			sdr.combineLevels(feed, append(eqLevels, l), levels)

			for _, l := range eqLevels {
				procedLvls[l] = true
			}

			procedLvls[l] = true
		}
	}

	// run multiple times to catch parent equivalencies
	for i := 0; i < 3; i++ {
		stoptimes := make(map[*gtfs.Stop][]*gtfs.StopTime, len(feed.Stops))
		stops := make(map[*gtfs.Stop][]*gtfs.Stop, len(feed.Stops))
		transfers := make(map[*gtfs.Stop][]gtfs.TransferKey, len(feed.Stops))
		pathways := make(map[*gtfs.Stop][]*gtfs.Pathway, len(feed.Stops))
		proced := make(map[*gtfs.Stop]bool, len(feed.Stops))

		chunks := sdr.getStopChunks(feed)

		// collect stop times that use stops
		for _, t := range feed.Trips {
			for i, st := range t.StopTimes {
				stoptimes[st.Stop()] = append(stoptimes[st.Stop()], &t.StopTimes[i])
			}
		}

		// collect stops that use stops as parents
		for _, s := range feed.Stops {
			if s.Parent_station != nil {
				stops[s.Parent_station] = append(stops[s.Parent_station], s)
			}
		}

		// collect transfers that use stop
		for tk := range feed.Transfers {
			transfers[tk.From_stop] = append(transfers[tk.From_stop], tk)
			if tk.From_stop != tk.To_stop {
				transfers[tk.To_stop] = append(transfers[tk.To_stop], tk)
			}
		}

		// collect pathways that use stop
		for _, p := range feed.Pathways {
			pathways[p.From_stop] = append(pathways[p.From_stop], p)
			if p.From_stop != p.To_stop {
				pathways[p.To_stop] = append(pathways[p.To_stop], p)
			}
		}

		i := 0

		for _, s := range feed.Stops {
			i += 1
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
				if sdr.stopEquals(s, stop, feed) {
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
	transfers map[*gtfs.Stop][]gtfs.TransferKey,
	pathways map[*gtfs.Stop][]*gtfs.Pathway) {
	// heuristic: use the stop with the most colons as the reference stop, to prefer
	// stops with global ID of the form de:54564:345:3 over something like 5542, and to
	// also prefer more specific global IDs. If the number of colons is equivalent,
	// user the shorter id. If the IDs also have the same length, order alphabetically and take
	// the first one
	ref := stops[0]

	for _, s := range stops {
		numColsS := sdr.numColons(s.Id)
		numColsRef := sdr.numColons(ref.Id)
		if numColsS > numColsRef || (numColsS == numColsRef && len(ref.Id) > len(s.Id)) || (numColsS == numColsRef && len(ref.Id) == len(s.Id) && s.Id < ref.Id) {
			ref = s
		}
	}

	for _, s := range stops {
		if s == ref {
			continue
		}

		for i, st := range stoptimes[s] {
			if st.Stop() == s {
				stoptimes[s][i].SetStop(ref)
				stoptimes[ref] = append(stoptimes[ref], stoptimes[s][i])
			}
		}

		for i, ps := range pstops[s] {
			if ps.Parent_station == s {
				pstops[s][i].Parent_station = ref
				pstops[ref] = append(pstops[ref], pstops[s][i])
			}
		}

		for _, tk := range transfers[s] {
			// update the  key
			tk_new := tk

			// at least one of the following two will be true
			if tk.From_stop == s {
				tk_new.From_stop = ref
			}

			if tk.To_stop == s {
				tk_new.To_stop = ref
			}

			if _, ok := feed.Transfers[tk_new]; !ok {
				feed.Transfers[tk_new] = feed.Transfers[tk]
				delete(feed.Transfers, tk)

				// add new transfer to transfer refs
				transfers[ref] = append(transfers[ref], tk_new)
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

		feed.DeleteStop(s.Id)
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

	if !sdr.Fuzzy {
		binary.LittleEndian.PutUint64(b, uint64(uintptr(unsafe.Pointer(s.Level))))
		h.Write(b)
	}

	binary.LittleEndian.PutUint64(b, uint64(s.Location_type))
	h.Write(b)

	binary.LittleEndian.PutUint64(b, uint64(s.Wheelchair_boarding))
	h.Write(b)

	if !sdr.Fuzzy {
		h.Write([]byte(s.Code))
	}

	if !sdr.Fuzzy {
		h.Write([]byte(s.Name))
	}

	h.Write([]byte(s.Desc))
	h.Write([]byte(s.Zone_id))
	h.Write([]byte(s.Timezone.GetTzString()))

	if !sdr.Fuzzy {
		h.Write([]byte(s.Platform_code))
	}

	return h.Sum32()
}

// Returns the feed's levels that are equivalent to level
func (sdr StopDuplicateRemover) getEquivalentLevels(lvl *gtfs.Level, feed *gtfsparser.Feed) []*gtfs.Level {
	ret := make([]*gtfs.Level, 0)

	for _, l := range feed.Levels {
		if l == lvl {
			continue
		}
		if _, ok := feed.Levels[l.Id]; !ok {
			continue
		}

		addFldsEq := true

		if !sdr.Fuzzy {
			for _, v := range feed.LevelsAddFlds {
				if v[l.Id] != v[lvl.Id] {
					addFldsEq = false
					break
				}
			}
		}

		if addFldsEq && l.Index == lvl.Index && l.Name == lvl.Name {
			ret = append(ret, l)
		}
	}

	return ret
}

// Combine a slice of equal levels into a single stop
func (sdr StopDuplicateRemover) combineLevels(feed *gtfsparser.Feed, levels []*gtfs.Level, stops map[*gtfs.Level][]*gtfs.Stop) {
	// heuristic: use the level with the shortest ID as 'reference'
	ref := levels[0]

	for _, l := range levels {
		if len(l.Id) < len(ref.Id) {
			ref = l
		}
	}

	for _, l := range levels {
		if l == ref {
			continue
		}

		for i, s := range stops[l] {
			if s.Level == l {
				stops[l][i].Level = ref
				stops[ref] = append(stops[ref], stops[l][i])
			}
		}

		feed.DeleteLevel(l.Id)
	}
}

// count number of colons in a string
func (sdr StopDuplicateRemover) numColons(str string) int {
	count := 0
	for _, c := range str {
		if c == ':' {
			count += 1
		}
	}
	return count
}

// Check if two stops are equal, distances under 1 m count as equal
func (sdr StopDuplicateRemover) stopEquals(a *gtfs.Stop, b *gtfs.Stop, feed *gtfsparser.Feed) bool {
	addFldsEq := true

	if !sdr.Fuzzy {
		for _, v := range feed.StopsAddFlds {
			if v[a.Id] != v[b.Id] {
				addFldsEq = false
				break
			}
		}
	}

	parentsEqual := a.Parent_station != nil && a.Parent_station == b.Parent_station

	if sdr.Fuzzy {
		distApprox := distSApprox(a, b)
		return ((distApprox <= sdr.DistThresholdStop/2 && parentsEqual) || a.Code == b.Code || len(a.Code) == 0 || len(b.Code) == 0) &&
			((distApprox <= sdr.DistThresholdStop/2 && parentsEqual) || a.Name == b.Name) &&
			a.Desc == b.Desc &&
			a.Zone_id == b.Zone_id &&
			(a.Url == b.Url || a.Url == nil || b.Url == nil) &&
			a.Location_type == b.Location_type &&
			a.Parent_station == b.Parent_station &&
			a.Timezone.Equals(b.Timezone) &&
			a.Wheelchair_boarding == b.Wheelchair_boarding &&
			(a.Level == b.Level || a.Level == nil || b.Level == nil) &&
			((distApprox <= sdr.DistThresholdStop/2 && parentsEqual && (len(a.Platform_code) == 0 || len(b.Platform_code) == 0)) || a.Platform_code == b.Platform_code) &&
			(distApprox <= sdr.DistThresholdStop || (a.Location_type == 1 && distApprox <= sdr.DistThresholdStation))
	}

	return addFldsEq && a.Code == b.Code &&
		a.Name == b.Name &&
		a.Desc == b.Desc &&
		a.Zone_id == b.Zone_id &&
		a.Url == b.Url &&
		a.Location_type == b.Location_type &&
		a.Parent_station == b.Parent_station &&
		a.Timezone.Equals(b.Timezone) &&
		a.Wheelchair_boarding == b.Wheelchair_boarding &&
		a.Level == b.Level &&
		a.Platform_code == b.Platform_code &&
		(distSApprox(a, b) <= sdr.DistThresholdStop || (a.Location_type == 1 && distSApprox(a, b) <= sdr.DistThresholdStation))
}
