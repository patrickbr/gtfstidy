// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"os"
)

// ServiceCalDatesRemover removes any entry in calendar_dates.txt by
// splitting services into continuous blocks
type ServiceCalDatesRem struct {
	ServiceMinimizer
	tidc uint
	sidc uint
}

// Run this ServiceMinimizer on some feed
func (sm ServiceCalDatesRem) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing calendar_dates.txt entries... ")
	calBefore, datesBefore := sm.countServices(feed)

	newServices := make(map[*gtfs.Service][]*gtfs.Service, 0)

	for _, s := range feed.Services {
		blocks := sm.getBlocks(feed, s)

		if len(blocks) == 1 {
			// change inplace
			s.SetStart_date(blocks[0].Start_date())
			s.SetEnd_date(blocks[0].End_date())
			s.SetRawDaymap(blocks[0].RawDaymap())
			s.SetExceptions(make(map[gtfs.Date]bool, 0))
		} else {
			newServices[s] = blocks
		}
	}

	for old, news := range newServices {
		for _, new := range news {
			feed.Services[new.Id()] = new
		}
		delete(feed.Services, old.Id())
	}

	newTrips := make([]*gtfs.Trip, 0)

	for _, trip := range feed.Trips {
		if val, ok := newServices[trip.Service]; ok {
			// change first trip in place
			trip.Service = val[0]

			for i := 1; i < len(val); i++ {
				newTrip := new(gtfs.Trip)

				newTrip.Service = val[i]
				newTrip.Bikes_allowed = trip.Bikes_allowed
				newTrip.Block_id = trip.Block_id
				newTrip.Direction_id = trip.Direction_id
				newTrip.Headsign = trip.Headsign

				newTrip.Id = sm.freeTripId(feed, trip.Id)

				newTrip.Route = trip.Route
				newTrip.Shape = trip.Shape
				newTrip.Short_name = trip.Short_name
				newTrip.Wheelchair_accessible = trip.Wheelchair_accessible
				newTrip.StopTimes = append([]gtfs.StopTime(nil), trip.StopTimes...)

				newTrips = append(newTrips, newTrip)
			}
		}
	}

	for _, trip := range newTrips {
		feed.Trips[trip.Id] = trip
	}

	calAfter, datesAfter := sm.countServices(feed)

	datesSign := ""
	calsSign := ""

	if datesAfter >= datesBefore {
		datesSign = "+"
	}

	if calAfter >= calBefore {
		calsSign = "+"
	}

	fmt.Fprintf(os.Stdout, "done. (%s%d calendar_dates.txt entries, %s%d calendar.txt entries)\n", datesSign, datesAfter-datesBefore, calsSign, calAfter-calBefore)
}

func (sm *ServiceCalDatesRem) getBlocks(feed *gtfsparser.Feed, s *gtfs.Service) []*gtfs.Service {
	ret := make([]*gtfs.Service, 0)

	first := s.GetFirstDefinedDate()
	last := s.GetLastDefinedDate()

	curBlockStart := first

	for first.GetTime().Before(last.GetTime()) || first.GetTime() == last.GetTime() {
		if s.IsActiveOn(first) && !s.Daymap(int(first.GetTime().Weekday())) {
			// if day is valid according to exception, but not according to map

			// add a single day span
			service_ex := new(gtfs.Service)
			service_ex.SetId(sm.freeServiceId(feed, s.Id()))
			service_ex.SetExceptions(make(map[gtfs.Date]bool, 0))
			service_ex.SetRawDaymap(0)

			service_ex.SetDaymap(int(first.GetTime().Weekday()), true)

			service_ex.SetStart_date(first)
			service_ex.SetEnd_date(first)

			ret = append(ret, service_ex)
		} else if !s.IsActiveOn(first) && s.Daymap(int(first.GetTime().Weekday())) {
			// if day is valid according to map, but not according to exception

			// if the current block start is the same as the currently checked date,
			// the open block has size 1, so we can skip it
			if first.GetTime() != curBlockStart.GetTime() {
				service := new(gtfs.Service)

				service.SetId(sm.freeServiceId(feed, s.Id()))
				service.SetExceptions(make(map[gtfs.Date]bool, 0))

				service.SetRawDaymap(s.RawDaymap())
				service.SetStart_date(curBlockStart)

				// end date is the date before the current date
				service.SetEnd_date(first.GetOffsettedDate(-1))

				ret = append(ret, service)
			}

			// new block start is the next day
			curBlockStart = first.GetOffsettedDate(1)
		}

		// if map and exception say the same, do nothing
		first = first.GetOffsettedDate(1)
	}

	// add last block, if open
	if (s.RawDaymap() > 0) && (curBlockStart.GetTime().Before(last.GetTime()) || curBlockStart.GetTime() == last.GetTime()) {
		service := new(gtfs.Service)
		service.SetId(sm.freeServiceId(feed, s.Id()))
		service.SetExceptions(make(map[gtfs.Date]bool, 0))

		service.SetRawDaymap(s.RawDaymap())
		service.SetStart_date(curBlockStart)

		// end date is the date before the current date
		service.SetEnd_date(first.GetOffsettedDate(-1))

		ret = append(ret, service)
	}

	// remove dates not in span from map
	ret2 := make([]*gtfs.Service, 0)
	for _, s := range ret {
		newmap := [7]bool{false, false, false, false, false, false, false}
		start := s.Start_date()
		end := s.End_date()

		for start.GetTime().Before(end.GetTime()) || start.GetTime() == end.GetTime() {
			if s.Daymap(int(start.GetTime().Weekday())) {
				newmap[start.GetTime().Weekday()] = true
			}
			start = start.GetOffsettedDate(1)
		}

		s.SetDaymap(0, newmap[0])
		s.SetDaymap(1, newmap[1])
		s.SetDaymap(2, newmap[2])
		s.SetDaymap(3, newmap[3])
		s.SetDaymap(4, newmap[4])
		s.SetDaymap(5, newmap[5])
		s.SetDaymap(6, newmap[6])

		if newmap[0] || newmap[1] || newmap[2] || newmap[3] || newmap[4] || newmap[5] || newmap[6] {
			ret2 = append(ret2, s)
		}
	}

	if len(ret2) == 0 {
		// special case: service was empty, re-add empty

		service := new(gtfs.Service)
		service.SetId(sm.freeServiceId(feed, s.Id()))
		service.SetExceptions(make(map[gtfs.Date]bool, 0))

		service.SetRawDaymap(0)
		service.SetStart_date(s.GetFirstDefinedDate())
		service.SetEnd_date(s.GetLastDefinedDate())

		ret2 = append(ret, service)
	}

	return ret2
}

// get a free trip id with the given prefix
func (sm *ServiceCalDatesRem) freeTripId(feed *gtfsparser.Feed, prefix string) string {
	for sm.tidc < ^uint(0) {
		sm.tidc += 1
		tid := prefix + fmt.Sprint(sm.tidc)
		if _, ok := feed.Trips[tid]; !ok {
			return tid
		}
	}
	panic(errors.New("Ran out of free trip ids."))
}

// get a free service id with the given prefix
func (sm *ServiceCalDatesRem) freeServiceId(feed *gtfsparser.Feed, prefix string) string {
	for sm.sidc < ^uint(0) {
		sm.sidc += 1
		sid := prefix + fmt.Sprint(sm.sidc)
		if _, ok := feed.Services[sid]; !ok {
			return sid
		}
	}
	panic(errors.New("Ran out of free service ids."))
}
