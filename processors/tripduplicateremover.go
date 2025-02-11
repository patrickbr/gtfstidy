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
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// TripDuplicateRemover merges semantically equivalent routes
type TripDuplicateRemover struct {
	Fuzzy       bool
	Aggressive  bool
	MaxDayDist  int
	serviceIdC  int
	serviceList map[*gtfs.Service][]uint64
	refDate     time.Time
	serviceRefs map[*gtfs.Service]int
}

type Overlap struct {
	Trip  *gtfs.Trip
	Dates []uint64
}

// Run this TripDuplicateRemover on some feed

// The philosophy is as follows:

// (1) Two trips are declared attribute equal, if all their attributes match (they also have the same route)
// (2) Two trips are declared stop-time equal, if they serve exactly the same stations (not stops, that is
// their station parents are considered!) at exactly the same times (excluding the arrival and departure times
// at the last stop)
// (3) Two trips are declared calendar-equal, if they run at precisely the same dates
// (4) Trip A is declared to contain trip B calendar-wise if A serves all the dates that B does
// (5) Two trips are declared to intersect calendar-wise if they have serving dates in common
// (6) In fuzzy mode, we don't check the attributes of the trip but assume that two trips serving the same stations
// at the same time and at the same dates are equal if their method of transportation is the same

// The duplicate removal then works like this:

// (1) In a first round, we find all trips that are attribute-equal (if non-fuzzy), stop-time equal and calendar equal.
// For these trips, a reference is chosen arbitrarily and merged with all others trips. Then the trip attributes
// are corrected (for example, if trips A and B are merged, A is the reference which did not allow bikes, but B did
// allow bikes, we allow bikes on the reference trip etc.)
// (2) In a second round, we find for each trip A all trips that are attribute-equal (if non-fuzzy), stop-time equal and
// which are contained calendar-wise in A. These trips are all deleted, and A is kept as a reference. No attribute
// updating is done (which, in fuzzy mode, may lead to some meta information being lost which was present in the
// contained trips), but if A had no shapes and any of the contained trips do, we use the shape for A
// (3) In a third round, we find for each trip A all trips that are attribute-equal (if non-fuzzy), stop-time equal and
// which are intersecting calendar-wise. Note that as equal and contained trips are deleted in (1) and (2), these
// intersections are non-trivial. We simply delete the intersection service days from A. If A was the only trip using
// the service of A, we can do this in-place. If the service is shared by another trip, we copy the service and update
// the copy. In both cases, the service is minimized subsequently using the service minimizer.

// In the last round, matching trips which are adjacent calendar-wise are merged

func (m TripDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant trips... ")
	bef := len(feed.Trips)

	m.serviceRefs = make(map[*gtfs.Service]int, 0)
	for _, t := range feed.Trips {
		m.serviceRefs[t.Service] += 1
	}

	m.serviceList = make(map[*gtfs.Service][]uint64)

	// infinity time
	m.refDate = time.Unix(1<<63-62135596801, 999999999)

	for _, s := range feed.Services {
		a := s.GetFirstDefinedDate()
		if a.GetTime().Before(m.refDate) {
			m.refDate = a.GetTime()
		}
	}

	for _, s := range feed.Services {
		m.writeServiceList(s)
	}

	for m.combineAllEqTrips(feed) {
	}

	for m.combineAllContainedTrips(feed) {
	}

	for m.combineAllOverlapTrips(feed) {
	}

	for i := 1; i <= m.MaxDayDist; i++ {
		for m.combineAllAdjTrips(feed, uint64(i), m.Aggressive) {
		}
	}

	// delete transfers
	feed.CleanTransfers()

	fmt.Fprintf(os.Stdout, "done. (-%d trips [-%.2f%%])\n",
		(bef - len(feed.Trips)),
		100.0*float64(bef-len(feed.Trips))/(float64(bef)+0.001))
}

func (m *TripDuplicateRemover) getParent(stop *gtfs.Stop) *gtfs.Stop {
	if stop.Location_type == 1 {
		return stop
	} else if stop.Location_type == 0 || stop.Location_type == 2 || stop.Location_type == 3 {
		if stop.Parent_station != nil {
			return stop.Parent_station
		}
	} else if stop.Location_type == 4 {
		if stop.Parent_station != nil {
			if stop.Parent_station.Parent_station != nil {
				return stop.Parent_station.Parent_station
			} else {
				return stop.Parent_station
			}
		}
	}

	return stop
}

// Combine a slice of adjacent trips into a single trip
func (m *TripDuplicateRemover) combineAdjTrips(feed *gtfsparser.Feed, ref *gtfs.Trip, trips []*gtfs.Trip) {
	if m.serviceRefs[ref.Service] != 1 {
		newService := new(gtfs.Service)
		newService.SetExceptions(make(map[gtfs.Date]bool, 0))
		newService.SetStart_date(ref.Service.Start_date())
		newService.SetEnd_date(ref.Service.End_date())

		for k, v := range ref.Service.Exceptions() {
			newService.Exceptions()[k] = v
		}
		newService.SetRawDaymap(ref.Service.RawDaymap())

		for ; ; m.serviceIdC++ {
			newService.SetId("merged" + strconv.Itoa(m.serviceIdC))
			if _, ok := feed.Services[newService.Id()]; !ok {
				break
			}
		}

		m.serviceRefs[ref.Service]--
		ref.Service = newService
		m.serviceRefs[ref.Service] = 1
		m.writeServiceList(ref.Service)
		feed.Services[ref.Service.Id()] = ref.Service
	}

	combServices := make([]*gtfs.Service, 0)

	for _, t := range trips {
		combServices = append(combServices, t.Service)
	}

	m.combineServices(combServices, ref.Service)

	for _, t := range trips {
		if t == ref {
			continue
		}

		if ref.Shape == nil && t.Shape != nil {
			ref.Shape = t.Shape

			// also update measurements
			for i := 0; i < len(ref.StopTimes); i++ {
				ref.StopTimes[i].SetShape_dist_traveled(t.StopTimes[i].Shape_dist_traveled())
			}
		}

		if t.Attributions != nil {
			if ref.Attributions == nil {
				sl := make([]*gtfs.Attribution, 0)
				ref.Attributions = &sl
			}
			for _, attr := range *t.Attributions {
				*ref.Attributions = append(*ref.Attributions, attr)
			}
		}

		for fld, v := range feed.TripsAddFlds {
			valT, okT := v[t.Id]
			_, okRef := v[ref.Id]
			if !okRef && okT {
				feed.TripsAddFlds[fld][ref.Id] = valT
			}
		}

		feed.DeleteTrip(t.Id)
		m.serviceRefs[t.Service]--
	}
}

// Combine a slice of contained trips into a single trip
func (m *TripDuplicateRemover) combineContTrips(feed *gtfsparser.Feed, ref *gtfs.Trip, trips []*gtfs.Trip) {
	for _, t := range trips {
		if t == ref {
			continue
		}

		if ref.Shape == nil && t.Shape != nil {
			ref.Shape = t.Shape

			// also update measurements
			for i := 0; i < len(ref.StopTimes); i++ {
				ref.StopTimes[i].SetShape_dist_traveled(t.StopTimes[i].Shape_dist_traveled())
			}
		}

		if t.Attributions != nil {
			if ref.Attributions == nil {
				sl := make([]*gtfs.Attribution, 0)
				ref.Attributions = &sl
			}
			for _, attr := range *t.Attributions {
				*ref.Attributions = append(*ref.Attributions, attr)
			}
		}

		feed.DeleteTrip(t.Id)
		m.serviceRefs[t.Service]--
	}
}

// Combine a slice of equal trips into a single trip
func (m *TripDuplicateRemover) combineEqTrips(feed *gtfsparser.Feed, ref *gtfs.Trip, trips []*gtfs.Trip) {
	for _, t := range trips {
		if t == ref {
			continue
		}

		if t.Attributions != nil {
			if ref.Attributions == nil {
				sl := make([]*gtfs.Attribution, 0)
				ref.Attributions = &sl
			}
			for _, attr := range *t.Attributions {
				*ref.Attributions = append(*ref.Attributions, attr)
			}
		}

		if ref.Bikes_allowed == 0 && t.Bikes_allowed > 0 {
			ref.Bikes_allowed = t.Bikes_allowed
		}

		if ref.Bikes_allowed == 2 && t.Bikes_allowed == 1 {
			ref.Bikes_allowed = 1
		}

		if ref.Wheelchair_accessible == 0 && t.Wheelchair_accessible > 0 {
			ref.Wheelchair_accessible = t.Wheelchair_accessible
		}

		if ref.Wheelchair_accessible == 2 && t.Wheelchair_accessible == 1 {
			ref.Wheelchair_accessible = 1
		}

		if ref.Shape == nil && t.Shape != nil {
			ref.Shape = t.Shape

			// also update measurements
			for i := 0; i < len(ref.StopTimes); i++ {
				ref.StopTimes[i].SetShape_dist_traveled(t.StopTimes[i].Shape_dist_traveled())
			}
		}

		if len(*ref.Headsign) == 0 {
			ref.Headsign = t.Headsign
		}

		if ref.Short_name == nil || len(*ref.Short_name) == 0 {
			ref.Short_name = t.Short_name
		}

		feed.DeleteTrip(t.Id)
		m.serviceRefs[t.Service]--
	}
}

// Exclude a list of overlaps from a trip
func (m *TripDuplicateRemover) excludeTrips(feed *gtfsparser.Feed, ref *gtfs.Trip, overlaps []Overlap) {
	for _, o := range overlaps {
		if ref.Shape == nil && o.Trip.Shape != nil {
			ref.Shape = o.Trip.Shape

			// also update measurements
			for i := 0; i < len(ref.StopTimes); i++ {
				ref.StopTimes[i].SetShape_dist_traveled(o.Trip.StopTimes[i].Shape_dist_traveled())
			}
		} else {
			break
		}
	}

	if m.serviceRefs[ref.Service] == 1 {
		// change inplace
		for _, o := range overlaps {
			for _, d := range o.Dates {
				date := m.getDateFromRefDay(d)
				ref.Service.SetExceptionTypeOn(date, 2)
			}
		}

		m.writeServiceList(ref.Service)

		// the service is now empty
		if len(m.serviceList[ref.Service]) == 0 {
			feed.DeleteTrip(ref.Id)
			m.serviceRefs[ref.Service]--
		}
	} else {
		newService := new(gtfs.Service)
		newService.SetExceptions(make(map[gtfs.Date]bool, 0))
		newService.SetStart_date(ref.Service.Start_date())
		newService.SetEnd_date(ref.Service.End_date())

		for k, v := range ref.Service.Exceptions() {
			newService.Exceptions()[k] = v
		}
		newService.SetRawDaymap(ref.Service.RawDaymap())

		for ; ; m.serviceIdC++ {
			newService.SetId("merged" + strconv.Itoa(m.serviceIdC))
			if _, ok := feed.Services[newService.Id()]; !ok {
				break
			}
		}

		for _, o := range overlaps {
			for _, d := range o.Dates {
				date := m.getDateFromRefDay(d)
				newService.SetExceptionTypeOn(date, 2)
			}
		}

		m.writeServiceList(newService)

		// the service is empty
		if len(m.serviceList[newService]) == 0 {
			feed.DeleteTrip(ref.Id)
			m.serviceRefs[ref.Service]--
			return
		}

		// otherwise, use the new service
		m.serviceRefs[ref.Service]--
		ref.Service = newService
		feed.Services[newService.Id()] = newService
		m.serviceRefs[newService] = 1
	}
}

// Check if two stops are equal
func (m *TripDuplicateRemover) stopEq(a *gtfs.Stop, b *gtfs.Stop) bool {
	return m.getParent(a) == m.getParent(b)
}

// Check if two trips are stop-times equal
func (m *TripDuplicateRemover) tripStEq(a *gtfs.Trip, b *gtfs.Trip) bool {
	if len(a.StopTimes) != len(b.StopTimes) {
		return false
	}

	// check departure times
	for i, aSt := range a.StopTimes {
		// NOTE: we don't check for the additional stop time attributes here
		bSt := b.StopTimes[i]

		if !m.stopEq(aSt.Stop(), bSt.Stop()) {
			return false
		}

		if i == 0 && aSt.Departure_time().Equals(bSt.Departure_time()) {
			continue
		}

		if i == len(a.StopTimes)-1 && aSt.Arrival_time().Equals(bSt.Arrival_time()) {
			continue
		}

		if aSt.Arrival_time().Equals(bSt.Arrival_time()) && aSt.Departure_time().Equals(bSt.Departure_time()) {
			continue
		}

		return false
	}

	return true
}

// Check if trip child is attribute equal to another trip
func (m *TripDuplicateRemover) tripAttrEq(a *gtfs.Trip, b *gtfs.Trip, feed *gtfsparser.Feed) bool {
	if !m.Fuzzy && a.Route != b.Route {
		return false
	}

	if m.Fuzzy && !m.typeComp(a.Route.Type, b.Route.Type) {
		return false
	}

	if (a.Frequencies != nil && len(*a.Frequencies) != 0) || (b.Frequencies != nil && len(*b.Frequencies) != 0) {
		// TODO: at the moment, don't combine trips with frequencies,
		// this is not yet implemented
		return false
	}

	if m.Fuzzy {
		return true
	}

	addFldsEq := true

	if !m.Fuzzy {
		for _, v := range feed.TripsAddFlds {
			if v[a.Id] != v[b.Id] {
				addFldsEq = false
				break
			}
		}
	}

	return addFldsEq && a.Wheelchair_accessible == b.Wheelchair_accessible &&
		a.Bikes_allowed == b.Bikes_allowed &&
		(a.Short_name == b.Short_name || (a.Short_name != nil && b.Short_name != nil && *a.Short_name == *b.Short_name)) &&
		(a.Headsign == b.Headsign || (m.Fuzzy && (a.Headsign == nil || b.Headsign == nil)) || (a.Headsign != nil && b.Headsign != nil && *a.Headsign == *b.Headsign || (m.Fuzzy && (strings.Contains(*b.Headsign, *a.Headsign) || strings.Contains(*a.Headsign, *b.Headsign))))) &&
		(a.Block_id == b.Block_id || (a.Block_id != nil && b.Block_id != nil && *a.Block_id == *b.Block_id))
}

// Check if trip child is equivalent to trip parent calendar-wise
func (m *TripDuplicateRemover) tripCalEq(a *gtfs.Trip, b *gtfs.Trip) bool {
	if a.Service == b.Service {
		// shortcut
		return true
	}

	if !m.Fuzzy {
		return false
		// we only merge in fuzzy mode if the services are not the same, but equal
	}

	// shortcut
	if !a.Service.Start_date().IsEmpty() && !b.Service.Start_date().IsEmpty() && len(a.Service.Exceptions()) == 0 && len(b.Service.Exceptions()) == 0 {
		return a.Service.Start_date() == b.Service.Start_date() && a.Service.End_date() == b.Service.End_date() && a.Service.RawDaymap() == b.Service.RawDaymap()
	}

	aDList := m.serviceList[a.Service]
	bDList := m.serviceList[b.Service]

	if len(aDList) != len(bDList) {
		return false
	}

	if aDList[len(aDList)-1] != bDList[len(bDList)-1] {
		return false
	}

	for i, v := range aDList {
		if v != bDList[i] {
			return false
		}
	}
	return true
}

// Check if trip child is contained in trip parent calendar-wise
func (m *TripDuplicateRemover) tripCalContained(child *gtfs.Trip, parent *gtfs.Trip) bool {
	childDList := m.serviceList[child.Service]
	parentDList := m.serviceList[parent.Service]

	if len(childDList) == 0 {
		// if the child has no service day, we trivially say it is contained
		return true
	}

	if len(parentDList) == 0 {
		return false
	}

	if len(childDList) > len(parentDList) {
		return false
	}

	is := intersect(childDList, parentDList)

	if len(is) != len(childDList) {
		return false
	}

	for i, d := range childDList {
		if d != is[i] {
			return false
		}
	}

	return true
}

// Check if trip child is adjacent to trip parent calendar-wise
func (m *TripDuplicateRemover) tripCalAdj(child *gtfs.Trip, parent *gtfs.Trip, maxdist uint64, aggressive bool) bool {
	if aggressive {
		return true
	}
	// only merge if daymap is equal, to avoid creating complicated services
	if !(!child.Service.Start_date().IsEmpty() && !parent.Service.Start_date().IsEmpty() && child.Service.RawDaymap() == parent.Service.RawDaymap()) {
		return false
	}

	childList := m.serviceList[child.Service]
	parentList := m.serviceList[parent.Service]

	if len(childList) == 0 || len(parentList) == 0 {
		return false
	}

	diffFront := parentList[0] - childList[len(childList)-1]
	diffBack := childList[0] - parentList[len(parentList)-1]

	return (diffFront > 0 && diffFront <= maxdist) || (diffBack > 0 && diffBack <= maxdist)
}

// Check if trip a is overlapping trip b calendar wise
func (m *TripDuplicateRemover) tripCalOverlap(a *gtfs.Trip, b *gtfs.Trip) []uint64 {
	ret := intersect(m.serviceList[a.Service], m.serviceList[b.Service])
	return ret
}

// Check if two routes are equal
func (m *TripDuplicateRemover) typeComp(a int16, b int16) bool {
	return gtfs.GetTypeFromExtended(a) == gtfs.GetTypeFromExtended(b)
}

func (m *TripDuplicateRemover) getTripChunks(feed *gtfsparser.Feed) [][][]*gtfs.Trip {
	numChunks := MaxParallelism()

	trips := make(map[uint64][]*gtfs.Trip)

	for _, t := range feed.Trips {
		if len(t.StopTimes) == 0 {
			continue
		}

		hash := m.tripHash(t)
		trips[hash] = append(trips[hash], t)
	}

	chunksize := (len(trips) + numChunks - 1) / numChunks
	chunks := make([][][]*gtfs.Trip, numChunks)
	curchunk := 0

	for hash := range trips {
		chunks[curchunk] = append(chunks[curchunk], make([]*gtfs.Trip, 0))

		for _, t := range trips[hash] {
			chunks[curchunk][len(chunks[curchunk])-1] = append(chunks[curchunk][len(chunks[curchunk])-1], t)
		}

		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	return chunks
}

func (m *TripDuplicateRemover) tripHash(t *gtfs.Trip) uint64 {
	h := fnv.New64a()

	b := make([]byte, 8)

	if len(t.StopTimes) > 0 {
		start := m.getParent(t.StopTimes[0].Stop())
		end := m.getParent(t.StopTimes[len(t.StopTimes)-1].Stop())

		binary.LittleEndian.PutUint64(b, uint64(uintptr(unsafe.Pointer(start))))
		h.Write(b)

		binary.LittleEndian.PutUint64(b, uint64(uintptr(unsafe.Pointer(end))))
		h.Write(b)

		binary.LittleEndian.PutUint64(b, uint64(len(t.StopTimes)))
		h.Write(b)

		binary.LittleEndian.PutUint64(b, uint64(t.StopTimes[0].Departure_time().SecondsSinceMidnight()))
		h.Write(b)

		binary.LittleEndian.PutUint64(b, uint64(t.StopTimes[len(t.StopTimes)-1].Arrival_time().SecondsSinceMidnight()))
		h.Write(b)

		binary.LittleEndian.PutUint64(b, uint64(gtfs.GetTypeFromExtended(t.Route.Type)))
		h.Write(b)
	}

	if !m.Fuzzy {
		binary.LittleEndian.PutUint64(b, uint64(uintptr(unsafe.Pointer(t.Route))))
		h.Write(b)

		if t.Short_name != nil {
			h.Write([]byte(*t.Short_name))
		} else {
			h.Write([]byte(""))
		}
		h.Write([]byte(*t.Headsign))
	}

	return h.Sum64()
}

func (m *TripDuplicateRemover) getDateFromRefDay(d uint64) gtfs.Date {
	return gtfs.GetGtfsDateFromTime((m.refDate.AddDate(0, 0, int(d))))
}

func (m *TripDuplicateRemover) combineServices(services []*gtfs.Service, ref *gtfs.Service) {
	dlist := m.serviceList[ref]

	// first collect all active dates of the services
	for _, serv := range services {
		if serv == ref {
			continue
		}

		dlist = merge(dlist, m.serviceList[serv])
	}

	if !ref.Start_date().IsEmpty() {
		// extend range and delete wrong dates
		for _, s := range services {
			first := m.getDateFromRefDay(m.serviceList[s][0])
			last := m.getDateFromRefDay(m.serviceList[s][len(m.serviceList[s])-1])

			if first.GetTime().Before(ref.Start_date().GetTime()) {
				ref.SetStart_date(first)
			}

			if last.GetTime().After(ref.End_date().GetTime()) {
				ref.SetEnd_date(last)
			}
		}

		// add all missing service dates
		for _, d := range dlist {
			date := m.getDateFromRefDay(d)
			if !ref.IsActiveOn(date) {
				ref.SetExceptionTypeOn(date, 1)
			}
		}

		m.writeServiceList(ref)
		dlistNew := m.serviceList[ref]

		// delete all wrong service dates
		for _, d := range diff(dlistNew, dlist) {
			date := m.getDateFromRefDay(d)
			ref.SetExceptionTypeOn(date, 2)
		}
	} else {
		// add all missing service dates
		for _, d := range dlist {
			date := m.getDateFromRefDay(d)
			if !ref.IsActiveOn(date) {
				ref.SetExceptionTypeOn(date, 1)
			}
		}
	}

	m.writeServiceList(ref)
}

func (m *TripDuplicateRemover) writeServiceList(s *gtfs.Service) {
	// make sure service list is empty first
	m.serviceList[s] = nil
	start := s.GetFirstActiveDate()
	end := s.GetLastActiveDate()
	endT := end.GetTime()

	for d := start; !d.GetTime().After(endT); d = d.GetOffsettedDate(1) {
		if s.IsActiveOn(d) {
			day := uint64(d.GetTime().Sub(m.refDate).Hours()) / 24
			m.serviceList[s] = append(m.serviceList[s], day)
		}
	}
}

func (m *TripDuplicateRemover) combineAllContainedTrips(feed *gtfsparser.Feed) bool {
	nchunks := m.getTripChunks(feed)

	rets := make([][][]*gtfs.Trip, len(nchunks))
	sem := make(chan empty, len(nchunks))

	for i, c := range nchunks {
		go func(j int, chunk [][]*gtfs.Trip) {
			processed := make(map[*gtfs.Trip]bool)
			for _, trips := range chunk {
				for _, ta := range trips {
					// skip already merged trips
					if _, ok := processed[ta]; ok {
						continue
					}
					written := false
					for _, tb := range trips {
						// skip equivalent trips
						if ta == tb {
							continue
						}

						// skip already merged trips
						if _, ok := processed[tb]; ok {
							continue
						}

						if m.tripAttrEq(ta, tb, feed) && m.tripStEq(ta, tb) {
							if m.tripCalContained(tb, ta) {
								if !written {
									rets[j] = append(rets[j], make([]*gtfs.Trip, 0))
									rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], ta)
									processed[ta] = true
									written = true
								}
								rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], tb)
								processed[tb] = true
							}
						}
					}
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(nchunks); i++ {
		<-sem
	}

	merged := false

	// combine all results
	for _, r := range rets {
		for _, trips := range r {
			m.combineContTrips(feed, trips[0], trips[1:])
			merged = true
		}
	}

	return merged
}

func (m *TripDuplicateRemover) combineAllEqTrips(feed *gtfsparser.Feed) bool {
	nchunks := m.getTripChunks(feed)

	rets := make([][][]*gtfs.Trip, len(nchunks))
	sem := make(chan empty, len(nchunks))

	for i, c := range nchunks {
		go func(j int, chunk [][]*gtfs.Trip) {
			processed := make(map[*gtfs.Trip]bool)
			for _, trips := range chunk {
				for _, ta := range trips {
					// skip already merged trips
					if _, ok := processed[ta]; ok {
						continue
					}
					written := false
					for _, tb := range trips {
						// skip equivalent trips
						if ta == tb {
							continue
						}

						// skip already merged trips
						if _, ok := processed[tb]; ok {
							continue
						}

						if m.tripAttrEq(ta, tb, feed) && m.tripStEq(ta, tb) {
							if m.tripCalEq(ta, tb) {
								if !written {
									rets[j] = append(rets[j], make([]*gtfs.Trip, 0))
									rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], ta)
									processed[ta] = true
									written = true
								}
								rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], tb)
								processed[tb] = true
							}
						}
					}
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(nchunks); i++ {
		<-sem
	}

	merged := false

	// combine all results
	for _, r := range rets {
		for _, trips := range r {
			m.combineEqTrips(feed, trips[0], trips[1:])
			merged = true
		}
	}

	return merged
}

func (m *TripDuplicateRemover) combineAllOverlapTrips(feed *gtfsparser.Feed) bool {
	nchunks := m.getTripChunks(feed)

	rets := make([][][]Overlap, len(nchunks))
	sem := make(chan empty, len(nchunks))

	for i, c := range nchunks {
		go func(j int, chunk [][]*gtfs.Trip) {
			processed := make(map[*gtfs.Trip]bool)
			for _, trips := range chunk {
				for _, ta := range trips {
					// skip already merged trips
					if _, ok := processed[ta]; ok {
						continue
					}
					written := false
					for _, tb := range trips {
						// skip equivalent trips
						if ta == tb {
							continue
						}

						// skip already merged trips
						if _, ok := processed[tb]; ok {
							continue
						}


						if m.tripAttrEq(ta, tb, feed) && m.tripStEq(ta, tb) {
							overlaps := m.tripCalOverlap(tb, ta)
							if len(overlaps) > 0 {
								if !written {
									rets[j] = append(rets[j], make([]Overlap, 0))
									rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], Overlap{ta, overlaps})
									processed[ta] = true
									written = true
								}
								rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], Overlap{tb, overlaps})
								processed[tb] = true
							}
						}
					}
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(nchunks); i++ {
		<-sem
	}

	merged := false

	// combine all results
	for _, r := range rets {
		for _, trips := range r {
			m.excludeTrips(feed, trips[0].Trip, trips[1:])
			merged = true
		}
	}

	return merged
}

func (m *TripDuplicateRemover) combineAllAdjTrips(feed *gtfsparser.Feed, maxDist uint64, aggressive bool) bool {
	nchunks := m.getTripChunks(feed)

	rets := make([][][]*gtfs.Trip, len(nchunks))
	sem := make(chan empty, len(nchunks))

	for i, c := range nchunks {
		go func(j int, chunk [][]*gtfs.Trip) {
			processed := make(map[*gtfs.Trip]bool)
			for _, trips := range chunk {
				for _, ta := range trips {
					// skip already merged trips
					if _, ok := processed[ta]; ok {
						continue
					}
					written := false
					for _, tb := range trips {
						// skip equivalent trips
						if ta == tb {
							continue
						}

						// skip already merged trips
						if _, ok := processed[tb]; ok {
							continue
						}

						if m.tripAttrEq(ta, tb, feed) && m.tripStEq(ta, tb) {
							if m.tripCalAdj(tb, ta, maxDist, aggressive) {
								if !written {
									rets[j] = append(rets[j], make([]*gtfs.Trip, 0))
									rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], ta)
									processed[ta] = true
									written = true
								}
								rets[j][len(rets[j])-1] = append(rets[j][len(rets[j])-1], tb)
								processed[tb] = true
							}
						}
					}
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(nchunks); i++ {
		<-sem
	}

	merged := false

	// combine all results
	for _, r := range rets {
		for _, trips := range r {
			m.combineAdjTrips(feed, trips[0], trips[1:])
			merged = true
		}
	}

	return merged
}
