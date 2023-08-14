// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
)

// FrequencyMinimizer minimizes trips, stop_times and frequencies by searching optimal covers for trip times.
type FrequencyMinimizer struct {
	MinHeadway int
	MaxHeadway int
}

type freqCandidate struct {
	matches  []int
	headways int
}

type progressionCover struct {
	progressions []freqCandidate
	coveredTrips map[*gtfs.Trip]empty
}

type tripWrapper struct {
	*gtfs.Trip
	t          gtfs.Time
	marked     bool
	sourceFreq *gtfs.Frequency
}

type tripWrappers struct {
	trips        []tripWrapper
	coveredTrips map[*gtfs.Trip]empty
}

func (a tripWrappers) Len() int      { return len(a.trips) }
func (a tripWrappers) Swap(i, j int) { a.trips[i], a.trips[j] = a.trips[j], a.trips[i] }
func (a tripWrappers) Less(i, j int) bool {
	return a.trips[i].t.SecondsSinceMidnight() < a.trips[j].t.SecondsSinceMidnight()
}

// Run the FrequencyMinimizer on a feed
func (m FrequencyMinimizer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Minimizing frequencies / stop times... ")
	processed := make(map[*gtfs.Trip]empty, 0)
	freqBef := 0
	for _, t := range feed.Trips {
		if t.Frequencies != nil {
			freqBef += len(*t.Frequencies)
		}
	}
	tripsBef := len(feed.Trips)

	// build a slice of trips for parallel processing
	tripsSl := make(map[*gtfs.Route]map[*gtfs.Service][]*gtfs.Trip, 0)
	for _, t := range feed.Trips {
		if _, in := tripsSl[t.Route]; !in {
			tripsSl[t.Route] = make(map[*gtfs.Service][]*gtfs.Trip, 1)
		}

		tripsSl[t.Route][t.Service] = append(tripsSl[t.Route][t.Service], t)
	}

	curAt := 0
	for _, t := range feed.Trips {
		curAt++
		if _, contained := processed[t]; contained {
			continue
		}

		if len(t.StopTimes) == 0 {
			continue
		}

		// trips time-independent equal to the current trip
		eqs := m.getTimeIndependentEquivalentTrips(t, tripsSl[t.Route][t.Service], feed)
		for _, t := range eqs.trips {
			processed[t.Trip] = empty{}
		}
		if len(eqs.trips) < 2 {
			continue
		}

		var cands progressionCover
		var packed []progressionCover

		var candsOverlapping progressionCover
		var packedOverlapping []progressionCover

		cands = m.getCover(eqs, false)
		packed = m.packCovers(cands, eqs)

		candsOverlapping = m.getCover(eqs, true)
		packedOverlapping = m.packCovers(candsOverlapping, eqs)

		if len(packed) > len(packedOverlapping) {
			packed = packedOverlapping
		}

		if len(packed) >= len(eqs.coveredTrips) {
			continue
		}

		// delete now redundant trips, update service
		// each "pack" is one trip
		suffixC := 1
		for _, indProgr := range packed {
			var curTrip *gtfs.Trip

			if suffixC > 1 {
				curTrip = new(gtfs.Trip)

				var newID string
				for true {
					newID = t.Id + "_" + strconv.FormatInt(int64(suffixC), 10)
					if _, in := feed.Trips[newID]; in {
						suffixC++
					} else {
						break
					}
				}

				// copy additional header
				for h := range feed.TripsAddFlds {
					feed.TripsAddFlds[h][newID] = feed.TripsAddFlds[h][t.Id]
				}

				for h := range feed.StopTimesAddFlds {
					feed.StopTimesAddFlds[h][newID] = feed.StopTimesAddFlds[h][t.Id]
				}

				curTrip.Id = newID
				feed.Trips[curTrip.Id] = curTrip
				processed[curTrip] = empty{}
				curTrip.Route = t.Route
				curTrip.Service = t.Service
				curTrip.Headsign = t.Headsign
				curTrip.Short_name = t.Short_name
				curTrip.Direction_id = t.Direction_id
				curTrip.Block_id = t.Block_id
				curTrip.Shape = t.Shape
				curTrip.Wheelchair_accessible = t.Wheelchair_accessible
				curTrip.Bikes_allowed = t.Bikes_allowed
				curTrip.StopTimes = make(gtfs.StopTimes, len(t.StopTimes))
				copy(curTrip.StopTimes, t.StopTimes)
			} else {
				curTrip = t
			}

			freqs := make([]*gtfs.Frequency, 0)
			curTrip.Frequencies = &freqs

			suffixC++

			smallestStartTime := eqs.trips[indProgr.progressions[0].matches[0]].t

			// add new frequencies
			for _, p := range indProgr.progressions {
				if len(p.matches) == 1 {
					/**
					* we can assume that progressions with 1 match are only
					* contained in single-progression-packs
					 */
					continue
				}
				if smallestStartTime.SecondsSinceMidnight() > eqs.trips[p.matches[0]].t.SecondsSinceMidnight() {
					smallestStartTime = eqs.trips[p.matches[0]].t
				}
				a := new(gtfs.Frequency)

				if eqs.trips[p.matches[0]].sourceFreq != nil {
					a.Exact_times = eqs.trips[p.matches[0]].sourceFreq.Exact_times
				} else {
					a.Exact_times = true
				}
				a.Start_time = eqs.trips[p.matches[0]].t
				a.End_time = m.getGtfsTimeFromSec(eqs.trips[p.matches[len(p.matches)-1]].t.SecondsSinceMidnight() + p.headways)
				a.Headway_secs = p.headways
				*curTrip.Frequencies = append(*curTrip.Frequencies, a)
			}
			m.remeasureStopTimes(curTrip, smallestStartTime)
		}

		// delete all other trips
		for _, trip := range eqs.trips {
			if trip.Id != t.Id {
				// don't delete the trip with the original id, we have used it again!
				feed.DeleteTrip(trip.Id)
			}
		}
	}

	// delete transfers
	feed.CleanTransfers()

	freqsSign := ""
	tripsSign := ""

	freqAfter := 0
	for _, t := range feed.Trips {
		if t.Frequencies != nil {
			freqAfter += len(*t.Frequencies)
		}
	}

	if freqAfter >= freqBef {
		freqsSign = "+"
	}

	if len(feed.Trips) >= tripsBef {
		tripsSign = "+"
	}

	if freqBef > 0 {
		fmt.Fprintf(os.Stdout, "done. (%s%d frequencies [%s%.2f%%], %s%d trips [%s%.2f%%])\n",
			freqsSign,
			freqAfter-freqBef,
			freqsSign,
			100.0*float64(freqAfter-freqBef)/(float64(freqBef)),
			tripsSign,
			len(feed.Trips)-tripsBef,
			tripsSign,
			100.0*float64(len(feed.Trips)-tripsBef)/(float64(tripsBef)+0.001))
	} else {
		fmt.Fprintf(os.Stdout, "done. (%s%d frequencies, %s%d trips [%s%.2f%%])\n",
			freqsSign,
			freqAfter-freqBef,
			tripsSign,
			len(feed.Trips)-tripsBef,
			tripsSign,
			100.0*float64(len(feed.Trips)-tripsBef)/(float64(tripsBef)+0.001))
	}
}

// Pack covers into non-overlapping progressions
func (m FrequencyMinimizer) packCovers(c progressionCover, t tripWrappers) []progressionCover {
	ret := make([]progressionCover, 0)
	singleTrips := make([]progressionCover, 0)
	ret = append(ret, progressionCover{make([]freqCandidate, 0), make(map[*gtfs.Trip]empty, 0)})

	for _, c := range c.progressions {
		if len(c.matches) == 1 {
			// handle single-match progressions separately (they should remain single trips)
			newCover := progressionCover{make([]freqCandidate, 0), make(map[*gtfs.Trip]empty, 0)}
			newCover.progressions = append(newCover.progressions, c)
			singleTrips = append(singleTrips, newCover)

			continue
		}

		// search for non-overlapping progression already on ret or insert new one
		inserted := false
		for i, existingCover := range ret {
			overlap := false
			for _, existingProg := range existingCover.progressions {
				if !(t.trips[existingProg.matches[0]].t.SecondsSinceMidnight() > t.trips[c.matches[len(c.matches)-1]].t.SecondsSinceMidnight() || t.trips[existingProg.matches[len(existingProg.matches)-1]].t.SecondsSinceMidnight() < t.trips[c.matches[0]].t.SecondsSinceMidnight()) {
					overlap = true
					break
				}
			}
			if !overlap {
				ret[i].progressions = append(ret[i].progressions, c)
				inserted = true
				break
			}
		}

		if !inserted {
			newCover := progressionCover{make([]freqCandidate, 0), make(map[*gtfs.Trip]empty, 0)}
			newCover.progressions = append(newCover.progressions, c)
			ret = append(ret, newCover)
		}
	}

	if len(ret) == 1 && len(ret[0].progressions) == 0 {
		return singleTrips
	}

	return append(ret, singleTrips...)
}

// Modified version of a CAP approximation algorithm proposed by
// Hannah Bast and Sabine Storandt in
// http://ad-publications.informatik.uni-freiburg.de/SIGSPATIAL_frequency_BS_2014.pdf
func (m FrequencyMinimizer) getCover(eqs tripWrappers, overlapping bool) progressionCover {
	for i := range eqs.trips {
		eqs.trips[i].marked = false
	}

	cand := progressionCover{make([]freqCandidate, 0), make(map[*gtfs.Trip]empty)}
	// sort them by start time
	sort.Sort(eqs)

	// collect possible frequency values contained in this collection
	freqs := m.getPossibleFreqs(eqs)

	minimumCoverSize := 2

	hasUnmarked := true
	for hasUnmarked {
		for minSize := len(eqs.trips); minSize > 0; minSize-- {
			// take the first non-marked trip and find the longest progression
			i := 0
			for ; i < len(eqs.trips)+1; i++ {
				if i >= len(eqs.trips) || !eqs.trips[i].marked {
					break
				}
			}

			if i >= len(eqs.trips) {
				// we are done for this trip
				hasUnmarked = false
				continue
			}

			startTime := eqs.trips[i].t
			curCand := freqCandidate{make([]int, 0), 0}
			curCand.matches = append(curCand.matches, i)
			for freq := range freqs {
				nextCand := freqCandidate{make([]int, 0), 0}
				nextCand.matches = append(nextCand.matches, i)

				for j := i + 1; j < len(eqs.trips); j++ {
					if eqs.trips[j].marked {
						if overlapping {
							continue
						} else {
							break
						}
					}

					freqEq := (eqs.trips[j].sourceFreq == eqs.trips[i].sourceFreq) || (eqs.trips[j].sourceFreq == nil && eqs.trips[i].sourceFreq.Exact_times) ||
						(eqs.trips[i].sourceFreq == nil && eqs.trips[j].sourceFreq.Exact_times) || (eqs.trips[i].sourceFreq != nil && eqs.trips[j].sourceFreq != nil && eqs.trips[i].sourceFreq.Exact_times == eqs.trips[j].sourceFreq.Exact_times)
					if freqEq && eqs.trips[j].t.SecondsSinceMidnight() == (startTime.SecondsSinceMidnight())+len(nextCand.matches)*freq {
						nextCand.matches = append(nextCand.matches, j)
						nextCand.headways = freq
					} else if !overlapping {
						break
					}
				}

				if len(nextCand.matches) > len(curCand.matches) && (len(nextCand.matches) >= minimumCoverSize || len(nextCand.matches) == 1) {
					curCand = nextCand
				}
			}

			// if the candidate is >= the min size, take it!
			if len(curCand.matches) >= minSize {
				cand.progressions = append(cand.progressions, curCand)
				// mark all trips as processed
				for _, t := range curCand.matches {
					eqs.trips[t].marked = true
					cand.coveredTrips[eqs.trips[t].Trip] = empty{}
				}
			}
		}
	}
	return cand
}

// Get possible frequencies from a collection of tripWrappers
func (m FrequencyMinimizer) getPossibleFreqs(tws tripWrappers) map[int]empty {
	ret := make(map[int]empty, 0)

	for i := range tws.trips {
		for ii := i + 1; ii < len(tws.trips); ii++ {
			fre := tws.trips[ii].t.SecondsSinceMidnight() - tws.trips[i].t.SecondsSinceMidnight()
			if fre != 0 && fre <= m.MaxHeadway && fre >= m.MinHeadway {
				ret[fre] = empty{}
			}
		}
	}
	return ret
}

// Get trips that are equal to trip without considering the absolute time values
func (m FrequencyMinimizer) getTimeIndependentEquivalentTrips(trip *gtfs.Trip, trips []*gtfs.Trip, feed *gtfsparser.Feed) tripWrappers {
	ret := tripWrappers{make([]tripWrapper, 0), make(map[*gtfs.Trip]empty, 0)}

	chunks := MaxParallelism()
	sem := make(chan empty, chunks)
	workload := int(math.Ceil(float64(len(trips)) / float64(chunks)))
	mutex := &sync.Mutex{}

	for j := 0; j < chunks; j++ {
		go func(j int) {
			for i := workload * j; i < workload*(j+1) && i < len(trips); i++ {
				t := trips[i]

				if t.Id == trip.Id || m.isTimeIndependentEqual(t, trip, feed) {
					if t.Frequencies == nil || len(*t.Frequencies) == 0 {
						mutex.Lock()
						ret.trips = append(ret.trips, tripWrapper{t, t.StopTimes[0].Arrival_time(), false, nil})
						ret.coveredTrips[t] = empty{}
						mutex.Unlock()
					} else {
						// expand frequencies
						for _, f := range *t.Frequencies {
							for s := f.Start_time.SecondsSinceMidnight(); s < f.End_time.SecondsSinceMidnight(); s = s + f.Headway_secs {
								mutex.Lock()
								ret.trips = append(ret.trips, tripWrapper{t, m.getGtfsTimeFromSec(s), false, f})
								ret.coveredTrips[t] = empty{}
								mutex.Unlock()
							}
						}
					}
				}
			}
			sem <- empty{}
		}(j)
	}

	for i := 0; i < chunks; i++ {
		<-sem
	}
	return ret
}

// Convert seconds since midnight to a GTFS time
func (m FrequencyMinimizer) getGtfsTimeFromSec(s int) gtfs.Time {
	return gtfs.Time{Hour: int8(s / 3600), Minute: int8((s - (s/3600)*3600) / 60), Second: int8(s - ((s / 60) * 60))}
}

// Check if two trips are equal without considering absolute stop times
func (m FrequencyMinimizer) isTimeIndependentEqual(a *gtfs.Trip, b *gtfs.Trip, feed *gtfsparser.Feed) bool {
	addFldsEq := true

	for _, v := range feed.TripsAddFlds {
		if v[a.Id] != v[b.Id] {
			addFldsEq = false
			break
		}
	}
	return addFldsEq && a.Route == b.Route && a.Service == b.Service && a.Headsign == b.Headsign &&
		a.Short_name == b.Short_name && a.Direction_id == b.Direction_id && a.Block_id == b.Block_id &&
		a.Shape == b.Shape && a.Wheelchair_accessible == b.Wheelchair_accessible &&
		a.Bikes_allowed == b.Bikes_allowed && m.hasSameRelStopTimes(a, b, feed)
}

// Remeasure a trips stop times by taking their relative values and changing the sequence to
// start with time
func (m FrequencyMinimizer) remeasureStopTimes(t *gtfs.Trip, time gtfs.Time) {
	diff := 0
	curArrDepDiff := 0
	for i := 0; i < len(t.StopTimes); i++ {
		curArrDepDiff = t.StopTimes[i].Departure_time().SecondsSinceMidnight() - t.StopTimes[i].Arrival_time().SecondsSinceMidnight()
		oldArrT := t.StopTimes[i].Arrival_time().SecondsSinceMidnight()

		t.StopTimes[i].SetArrival_time(m.getGtfsTimeFromSec(time.SecondsSinceMidnight() + diff))
		t.StopTimes[i].SetDeparture_time(m.getGtfsTimeFromSec(time.SecondsSinceMidnight() + diff + curArrDepDiff))

		if i < len(t.StopTimes)-1 {
			diff += t.StopTimes[i+1].Arrival_time().SecondsSinceMidnight() - oldArrT
		}
	}
}

// true if two trips share the same stops in the same order with the same
// relative stop times
func (m FrequencyMinimizer) hasSameRelStopTimes(a *gtfs.Trip, b *gtfs.Trip, feed *gtfsparser.Feed) bool {
	// handle trivial cases
	if len(a.StopTimes) != len(b.StopTimes) {
		return false
	}

	if len(a.StopTimes) == 0 && len(b.StopTimes) == 0 {
		return true
	}

	var aPrev *gtfs.StopTime
	var bPrev *gtfs.StopTime

	for i := range a.StopTimes {
		addFldsEq := true

		for _, v := range feed.StopTimesAddFlds {
			if v[a.Id][a.StopTimes[i].Sequence()] != v[b.Id][a.StopTimes[i].Sequence()] {
				addFldsEq = false
				break
			}
		}

		if !(addFldsEq && a.StopTimes[i].Stop() == b.StopTimes[i].Stop() &&
			a.StopTimes[i].Headsign() == b.StopTimes[i].Headsign() &&
			a.StopTimes[i].Pickup_type() == b.StopTimes[i].Pickup_type() && a.StopTimes[i].Drop_off_type() == b.StopTimes[i].Drop_off_type() && a.StopTimes[i].Continuous_drop_off() == b.StopTimes[i].Continuous_drop_off() && a.StopTimes[i].Continuous_pickup() == b.StopTimes[i].Continuous_pickup() &&
			((math.IsNaN(float64(a.StopTimes[i].Shape_dist_traveled())) && math.IsNaN(float64(b.StopTimes[i].Shape_dist_traveled()))) || FloatEquals(a.StopTimes[i].Shape_dist_traveled(), b.StopTimes[i].Shape_dist_traveled(), 0.01)) && a.StopTimes[i].Timepoint() == b.StopTimes[i].Timepoint()) {
			return false
		}
		if i != 0 {
			if a.StopTimes[i].Arrival_time().SecondsSinceMidnight()-aPrev.Arrival_time().SecondsSinceMidnight() != b.StopTimes[i].Arrival_time().SecondsSinceMidnight()-bPrev.Arrival_time().SecondsSinceMidnight() {
				return false
			}
			if a.StopTimes[i].Departure_time().SecondsSinceMidnight()-aPrev.Departure_time().SecondsSinceMidnight() != b.StopTimes[i].Departure_time().SecondsSinceMidnight()-bPrev.Departure_time().SecondsSinceMidnight() {
				return false
			}
		}

		aPrev = &a.StopTimes[i]
		bPrev = &b.StopTimes[i]
	}
	return true
}
