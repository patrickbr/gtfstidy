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

type empty struct{}

func hasBit(n uint, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

/**
 * Minimizes services by finding optimal calendar.txt and
 * calendar_dates.txt coverages.
 */
func (minimizer ServiceMinimizer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Minimizing services...\n")
	sem := make(chan empty, len(feed.Services))

	// try to do this in parallel
	for _, s := range feed.Services {
		go func(s *gtfs.Service) {
			minimizer.perfectMinimize(s)
			sem <- empty{}
		}(s)
	}

	// wait for goroutines to finish
	for i := 0; i < len(feed.Services); i++ {
		<-sem
	}
}

func (m ServiceMinimizer) perfectMinimize(service *gtfs.Service) ([7]bool, uint) {
	/**
	 *	TODO: maybe find a more performant approximation algorithm for this. It
	 *  feels like this problem could be reduced to SetCover (making it NP-complete),
	 *  but I am not 100% sure and have no proof / reduction method atm
	 **/

	start, end := m.getDateRange(service)

	startTime := start.GetTime()
	endTime := end.GetTime()

	e := ^uint(0) // "infinity"
	bestMap := uint(0)
	bestA := 0
	bestB := 0

	activeOn := make([]bool, 0)

	// build active map once for faster lookup later on

	// start and end at full weeks
	startTimeAm := startTime.AddDate(0, 0, -int(startTime.Weekday()))
	endTimeAm := endTime.AddDate(0, 0, 6-int(endTime.Weekday()))
	for d := m.getGtfsDateFromTime(startTimeAm); !d.GetTime().After(endTimeAm); d = m.getNextDate(d) {
		activeOn = append(activeOn, service.IsActiveOn(d))
	}

	l := len(activeOn)

	for a := 0; a < l; a = a + 7 {
		for b := l - 1; b > a; b = b - 7 {
			for d := uint(1); d < 128; d++ {
				c := m.countExceptions(service, activeOn, d, startTime, endTime, startTimeAm, a, b)
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
	newExceptions := make([]*gtfs.ServiceException, 0)

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
				ex := new(gtfs.ServiceException)
				ex.Date = gtfsD
				ex.Type = 1
				newExceptions = append(newExceptions, ex)
			}
		} else {
			if newMap[int(gtfsD.GetTime().Weekday())] {
				if !service.IsActiveOn(gtfsD) {
					ex := new(gtfs.ServiceException)
					ex.Date = gtfsD
					ex.Type = 2
					newExceptions = append(newExceptions, ex)
				}
			} else {
				if service.IsActiveOn(gtfsD) {
					ex := new(gtfs.ServiceException)
					ex.Date = gtfsD
					ex.Type = 1
					newExceptions = append(newExceptions, ex)
				}
			}
		}
	}

	service.Exceptions = newExceptions
	service.Start_date = m.getGtfsDateFromTime(newBegin)
	service.End_date = m.getGtfsDateFromTime(newEnd)
	service.Daymap = newMap

	return newMap, e
}

func (m ServiceMinimizer) countExceptions(s *gtfs.Service, actmap []bool, bm uint, start time.Time, end time.Time, startActMap time.Time, a int, b int) uint {
	ret := uint(0)

	l := len(actmap)
	for d := 0; d < l; d++ {
		checkD := startActMap.AddDate(0, 0, d)
		if checkD.Before(start) || checkD.After(end) {
			continue
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
			} else {
				if actmap[d] {
					ret++
				}
			}
		}
	}

	return ret
}

func (minimizer ServiceMinimizer) getDateRange(service *gtfs.Service) (gtfs.Date, gtfs.Date) {
	first := service.GetFirstDefinedDate()
	last := service.GetLastDefinedDate()

	for (first.GetTime().Before(last.GetTime())) && !service.IsActiveOn(first) {
		first = minimizer.getNextDate(first)
	}

	for (last.GetTime().After(first.GetTime())) && !service.IsActiveOn(last) {
		last = minimizer.getPrevDate(last)
	}

	return first, last
}

func (minimizer ServiceMinimizer) getGtfsDateFromTime(t time.Time) gtfs.Date {
	return gtfs.Date{int8(t.Day()), int8(t.Month()), int16(t.Year())}
}

func (minimizer ServiceMinimizer) getNextDate(d gtfs.Date) gtfs.Date {
	return minimizer.getGtfsDateFromTime((d.GetTime().AddDate(0, 0, 1)))
}

func (minimizer ServiceMinimizer) getPrevDate(d gtfs.Date) gtfs.Date {
	return minimizer.getGtfsDateFromTime((d.GetTime().AddDate(0, 0, -1)))
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
