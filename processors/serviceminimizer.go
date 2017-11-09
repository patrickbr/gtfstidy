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

type ServiceMinimizer struct {
}

type ServiceException struct {
	Date gtfs.Date
	Type int8
}

type DateRange struct {
	Start gtfs.Date
	End   gtfs.Date
}

func hasBit(n uint, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

/**
 * Minimizes services by finding optimal calendar.txt and
 * calendar_dates.txt coverages.
 */
func (minimizer ServiceMinimizer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Minimizing services... ")
	calBefore, datesBefore := minimizer.countServices(feed)

	full := len(feed.Services)
	count := 0

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

	sem := make(chan empty, len(feed.Services))
	for _, c := range chunks {
		go func(chunk []*gtfs.Service) {
			for _, s := range chunk {
				fmt.Fprintf(os.Stdout, "@ %d/%d\n", count, full)
				minimizer.perfectMinimize(s)

				count++
			}
			sem <- empty{}
		}(c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	calAfter, datesAfter := minimizer.countServices(feed)

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

func (m ServiceMinimizer) perfectMinimize(service *gtfs.Service) {
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

	activeOn := make([]bool, 0)
	activeOnDate := make(map[gtfs.Date]bool, 0)

	// build active map once for faster lookup later on

	// start and end at full weeks
	startTimeAm := startTime.AddDate(0, 0, -int(startTime.Weekday()))
	endTimeAm := endTime.AddDate(0, 0, 6-int(endTime.Weekday()))
	for d := m.getGtfsDateFromTime(startTimeAm); !d.GetTime().After(endTimeAm); d = m.getNextDate(d) {
		act := service.IsActiveOn(d)
		activeOn = append(activeOn, act)
		activeOnDate[d] = act
	}

	daysNotMatched := make([]int, 128)
	for d := uint(1); d < 128; d++ {
		for i := 0; i < 7; i++ {
			if service.Daymap[i] && !hasBit(d, uint(i)) {
				daysNotMatched[d]++
			}
		}
	}

	l := len(activeOn)

	for a := 0; a < l; a = a + 7 {
		for b := l - 1; b > a; b = b - 7 {
			for d := uint(1); d < 128; d++ {
				fullWeekCoverage := ((b - a) - 7) / 7
				minExc := fullWeekCoverage*daysNotMatched[d] - len(service.Exceptions)

				if minExc > -1 && uint(minExc) > e {
					continue
				}

				c := m.countExceptions(service, activeOn, d, startTime, endTime, startTimeAm, a, b, e)
				if c < e {
					e = c
					bestMap = d
					bestA = a
					bestB = b
				}
			}
		}
	}

	newMap := [7]bool{hasBit(bestMap, 0), hasBit(bestMap, 1), hasBit(bestMap, 2), hasBit(bestMap, 3), hasBit(bestMap, 4), hasBit(bestMap, 5), hasBit(bestMap, 6)}
	newBegin := startTime.AddDate(0, 0, bestA)
	newEnd := startTime.AddDate(0, 0, bestB)
	newExceptions := make([]*ServiceException, 0)

	for !newBegin.After(newEnd) && !service.IsActiveOn(m.getGtfsDateFromTime(newBegin)) {
		newBegin = m.getNextDateTime(newBegin)
	}

	for !newEnd.Before(newBegin) && !service.IsActiveOn(m.getGtfsDateFromTime(newEnd)) {
		newEnd = m.getPrevDateTime(newEnd)
	}

	if newBegin == newEnd {
		// dont allow single day maps, use exceptions for this
		newMap = [7]bool{false, false, false, false, false, false, false}
	}

	for st := start.GetTime(); !st.After(end.GetTime()); st = m.getNextDateTime(st) {
		gtfsD := m.getGtfsDateFromTime(st)
		if st.Before(newBegin) || st.After(newEnd) {
			if service.IsActiveOn(gtfsD) {
				ex := new(ServiceException)
				ex.Date = gtfsD
				ex.Type = 1
				newExceptions = append(newExceptions, ex)
			}
		} else {
			if newMap[int(gtfsD.GetTime().Weekday())] {
				if !service.IsActiveOn(gtfsD) {
					ex := new(ServiceException)
					ex.Date = gtfsD
					ex.Type = 2
					newExceptions = append(newExceptions, ex)
				}
			} else {
				if service.IsActiveOn(gtfsD) {
					ex := new(ServiceException)
					ex.Date = gtfsD
					ex.Type = 1
					newExceptions = append(newExceptions, ex)
				}
			}
		}
	}

	service.Exceptions = make(map[gtfs.Date]int8, 0)

	for _, e := range newExceptions {
		service.Exceptions[e.Date] = e.Type
	}

	service.Start_date = m.getGtfsDateFromTime(newBegin)
	service.End_date = m.getGtfsDateFromTime(newEnd)
	service.Daymap = newMap
}

func (m ServiceMinimizer) countExceptions(s *gtfs.Service, actmap []bool, bm uint, start time.Time, end time.Time, startActMap time.Time, a int, b int, max uint) uint {
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

func (minimizer ServiceMinimizer) getGtfsDateFromTime(t time.Time) gtfs.Date {
	return gtfs.Date{int8(t.Day()), int8(t.Month()), int16(t.Year())}
}

func (minimizer ServiceMinimizer) getNextDate(d gtfs.Date) gtfs.Date {
	return d.GetOffsettedDate(1)
}

func (minimizer ServiceMinimizer) getPrevDate(d gtfs.Date) gtfs.Date {
	return d.GetOffsettedDate(-1)
}

func (minimizer ServiceMinimizer) getNextDateTime(t time.Time) time.Time {
	return t.AddDate(0, 0, 1)
}

func (minimizer ServiceMinimizer) getPrevDateTime(t time.Time) time.Time {
	return t.AddDate(0, 0, -1)
}

func (minimizer ServiceMinimizer) getNextDateTimeWeek(t time.Time) time.Time {
	return t.AddDate(0, 0, 7-int(t.Weekday()))
}

func (minimizer ServiceMinimizer) getPrevDateTimeWeek(t time.Time) time.Time {
	return t.AddDate(0, 0, -(7 - int(t.Weekday())))
}

func (minimizer ServiceMinimizer) countServices(feed *gtfsparser.Feed) (int, int) {
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
