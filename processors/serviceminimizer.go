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
	"os"
	"time"
)

// ServiceMinimizer minimizes services by finding optimal calendar.txt and
// calendar_dates.txt coverages.
type ServiceMinimizer struct {
}

type serviceException struct {
	Date gtfs.Date
	Type int8
}

// DateRange specified a date range from Start to End
type DateRange struct {
	Start gtfs.Date
	End   gtfs.Date
}

func hasBit(n uint, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

// Run this ServiceMinimizer on some feed
func (sm ServiceMinimizer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Minimizing services... ")
	calBefore, datesBefore := sm.countServices(feed)

	numchunks := MaxParallelism()
	chunksize := (len(feed.Services) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Service, numchunks)

	curchunk := 0
	for _, s := range feed.Services {
		chunks[curchunk] = append(chunks[curchunk], s)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	sem := make(chan empty, len(chunks))
	for _, c := range chunks {
		go func(chunk []*gtfs.Service) {
			for _, s := range chunk {
				sm.perfectMinimize(s)
			}
			sem <- empty{}
		}(c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
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

	fmt.Fprintf(os.Stdout, "done. (%s%d calendar_dates.txt entries [%s%.2f%%], %s%d calendar.txt entries [%s%.2f%%])\n",
		datesSign,
		datesAfter-datesBefore,
		datesSign,
		100.0*(float64(datesAfter-datesBefore))/(float64(datesBefore)+0.001),
		calsSign,
		calAfter-calBefore,
		calsSign,
		100.0*(float64(calAfter-calBefore))/(float64(calBefore)+0.001))
}

func (sm ServiceMinimizer) perfectMinimize(service *gtfs.Service) {
	/**
	 *	TODO: maybe find a more performant approximation algorithm for this. It
	 *  feels like this problem could be reduced to SetCover (making it NP-complete),
	 *  but I am not 100% sure and have no proof / reduction method atm
	 **/

	dRange := GetDateRange(service)

	start := dRange.Start
	end := dRange.End

	startTime := start.GetTime()
	endTime := end.GetTime()

	e := ^uint(0)
	bestMap := uint(0)
	bestA := 0
	bestB := 0

	// build active map once for faster lookup later on
	// start and end at full weeks
	startTimeAm := startTime.AddDate(0, 0, -int(startTime.Weekday()))
	endTimeAm := endTime.AddDate(0, 0, 6-int(endTime.Weekday()))

	activeOn := sm.getActiveOnMap(startTimeAm, endTimeAm, service)
	daysNotMatched := sm.getDaysNotMatched(service)
	l := len(activeOn)

	for a := 0; a < l; a = a + 7 {
		for b := l - 1; b > a; b = b - 7 {
			for d := uint(1); d < 128; d++ {
				fullWeekCoverage := ((b - a) - 7) / 7
				minExc := fullWeekCoverage*daysNotMatched[d] - len(service.Exceptions)

				if minExc > -1 && uint(minExc) > e {
					continue
				}

				c := sm.countExceptions(service, activeOn, d, startTime, endTime, startTimeAm, a, b, e)
				if c < e {
					e = c
					bestMap = d
					bestA = a
					bestB = b
				}
			}
		}
	}

	sm.updateService(service, bestMap, bestA, bestB, startTime, endTime, start, end)
}

func (sm ServiceMinimizer) countExceptions(s *gtfs.Service, actmap []bool, bm uint, start time.Time, end time.Time, startActMap time.Time, a int, b int, max uint) uint {
	ret := uint(0)
	l := len(actmap)

	for d := 0; d < l; d++ {
		if ret >= max {
			return max
		}

		checkD := startActMap.AddDate(0, 0, d)
		if checkD.Before(start) {
			continue
		}
		if checkD.After(end) {
			break
		}
		if d < a || d > b {
			// we are out of the weekmap span
			if actmap[d] {
				ret++
			}
		} else {
			// we are in the weekmap span
			if hasBit(bm, uint(d%7)) {
				if !actmap[d] {
					ret++
				}
			} else if actmap[d] {
				ret++
			}
		}
	}

	return ret
}

// GetDateRange returns the active date range of a gtfs.Service
func GetDateRange(service *gtfs.Service) DateRange {
	first := service.GetFirstDefinedDate()
	last := service.GetLastDefinedDate()

	for (first.GetTime().Before(last.GetTime())) && !service.IsActiveOn(first) {
		first = first.GetOffsettedDate(1)
	}

	for (last.GetTime().After(first.GetTime())) && !service.IsActiveOn(last) {
		last = last.GetOffsettedDate(-1)
	}

	return DateRange{first, last}
}

// GetActDays returns the number of active days of a gtfs.Service
func GetActDays(service *gtfs.Service) int {
	first := service.GetFirstDefinedDate()
	last := service.GetLastDefinedDate()
	count := 0

	for first.GetTime().Before(last.GetTime()) || first.GetTime() == last.GetTime() {
		if service.IsActiveOn(first) {
			count++
		}
		first = first.GetOffsettedDate(1)
	}

	return count
}

func (sm ServiceMinimizer) getGtfsDateFromTime(t time.Time) gtfs.Date {
	return gtfs.Date{Day: int8(t.Day()), Month: int8(t.Month()), Year: int16(t.Year())}
}

func (sm ServiceMinimizer) getNextDate(d gtfs.Date) gtfs.Date {
	return d.GetOffsettedDate(1)
}

func (sm ServiceMinimizer) getPrevDate(d gtfs.Date) gtfs.Date {
	return d.GetOffsettedDate(-1)
}

func (sm ServiceMinimizer) getNextDateTime(t time.Time) time.Time {
	return t.AddDate(0, 0, 1)
}

func (sm ServiceMinimizer) getPrevDateTime(t time.Time) time.Time {
	return t.AddDate(0, 0, -1)
}

func (sm ServiceMinimizer) getNextDateTimeWeek(t time.Time) time.Time {
	return t.AddDate(0, 0, 7-int(t.Weekday()))
}

func (sm ServiceMinimizer) getPrevDateTimeWeek(t time.Time) time.Time {
	return t.AddDate(0, 0, -(7 - int(t.Weekday())))
}

func (sm ServiceMinimizer) countServices(feed *gtfsparser.Feed) (int, int) {
	cals := 0
	dates := 0

	for _, s := range feed.Services {
		dates += len(s.Exceptions)
		if s.Daymap[0] || s.Daymap[1] || s.Daymap[2] || s.Daymap[3] || s.Daymap[4] || s.Daymap[5] || s.Daymap[6] {
			cals++
		}
	}
	return cals, dates
}

func (sm ServiceMinimizer) getDaysNotMatched(service *gtfs.Service) [128]int {
	var ret [128]int
	for d := uint(1); d < 128; d++ {
		for i := 0; i < 7; i++ {
			if service.Daymap[i] && !hasBit(d, uint(i)) {
				ret[d]++
			}
		}
	}

	return ret
}

func (sm ServiceMinimizer) getActiveOnMap(startTimeAm time.Time, endTimeAm time.Time, service *gtfs.Service) []bool {
	activeOn := make([]bool, 0)
	for d := sm.getGtfsDateFromTime(startTimeAm); !d.GetTime().After(endTimeAm); d = sm.getNextDate(d) {
		act := service.IsActiveOn(d)
		activeOn = append(activeOn, act)
	}
	return activeOn
}

func (sm ServiceMinimizer) updateService(service *gtfs.Service, bestMap uint, bestA int, bestB int, startTime time.Time, endTime time.Time, start gtfs.Date, end gtfs.Date) {
	newMap := [7]bool{hasBit(bestMap, 0),
		hasBit(bestMap, 1),
		hasBit(bestMap, 2),
		hasBit(bestMap, 3),
		hasBit(bestMap, 4),
		hasBit(bestMap, 5),
		hasBit(bestMap, 6)}
	newBegin := startTime.AddDate(0, 0, bestA)
	newEnd := startTime.AddDate(0, 0, bestB)
	fmt.Println(newBegin, newEnd)
	newExceptions := make([]*serviceException, 0)

	// crop at the beginning
	for newEnd.After(newBegin) && !service.IsActiveOn(sm.getGtfsDateFromTime(newBegin)) {
		newBegin = sm.getNextDateTime(newBegin)
	}

	// crop at the end
	for newBegin.Before(newEnd) && !service.IsActiveOn(sm.getGtfsDateFromTime(newEnd)) {
		newEnd = sm.getPrevDateTime(newEnd)
	}

	if newBegin == newEnd {
		// dont allow single day maps, use exceptions for this
		newMap = [7]bool{false, false, false, false, false, false, false}
	}

	for st := start.GetTime(); !st.After(end.GetTime()); st = sm.getNextDateTime(st) {
		gtfsD := sm.getGtfsDateFromTime(st)
		if st.Before(newBegin) || st.After(newEnd) {
			if service.IsActiveOn(gtfsD) {
				ex := new(serviceException)
				ex.Date = gtfsD
				ex.Type = 1
				newExceptions = append(newExceptions, ex)
			}
		} else {
			if newMap[int(gtfsD.GetTime().Weekday())] {
				if !service.IsActiveOn(gtfsD) {
					ex := new(serviceException)
					ex.Date = gtfsD
					ex.Type = 2
					newExceptions = append(newExceptions, ex)
				}
			} else {
				if service.IsActiveOn(gtfsD) {
					ex := new(serviceException)
					ex.Date = gtfsD
					ex.Type = 1
					newExceptions = append(newExceptions, ex)
				}
			}
		}
	}

	service.Exceptions = make(map[gtfs.Date]bool, 0)

	for _, e := range newExceptions {
		service.SetExceptionTypeOn(e.Date, e.Type)
	}

	service.Start_date = sm.getGtfsDateFromTime(newBegin)
	service.End_date = sm.getGtfsDateFromTime(newEnd)
	service.Daymap = newMap
}
