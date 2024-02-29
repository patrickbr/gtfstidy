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
	"golang.org/x/exp/slices"
	"os"
	"sort"
	"strconv"
)

type DayType struct {
	Dates []gtfs.Date
	Trips []*gtfs.Trip
}

// ServiceNonOverlapper constructs day-wise non-overlapping trips. Basically, this works as
// follows: uniqe day types are constructed for each day of the week. A day type is one DOW
// on which *excactly* the same trips are served. Similary day types are than aggreated,
// and outfitted with an ID "<Weekday> (WW<list of calendar weeks served)".
type ServiceNonOverlapper struct {
	DayNames []string
	YearWeekName string
}

// Run this ServiceMinimizer on some feed
func (sm ServiceNonOverlapper) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Creating distinct, non-overlapping services... ")

	days := make([]map[gtfs.Date][]*gtfs.Trip, 7)
	day_types := make([][]DayType, 7)

	for i := 0; i < 7; i++ {
		days[i] = make(map[gtfs.Date][]*gtfs.Trip)
		day_types[i] = make([]DayType, 0)
	}

	for _, t := range feed.Trips {
		cur := t.Service.GetFirstDefinedDate()
		last := t.Service.GetLastDefinedDate()

		for cur.GetTime().Before(last.GetTime()) || cur.GetTime() == last.GetTime() {
			if t.Service.IsActiveOn(cur) {
				days[cur.GetTime().Weekday()][cur] = append(days[cur.GetTime().Weekday()][cur], t)
			}
			cur = cur.GetOffsettedDate(1)
		}
	}

	for wd, _ := range days {
		for day, _ := range days[wd] {
			sort.Slice(days[wd][day], func(i, j int) bool {
				return days[wd][day][i].Id < days[wd][day][j].Id
			})
		}
	}

	// collect day types
	for wd, _ := range days {
		for day, trips := range days[wd] {
			found := false
			for i, existing := range day_types[wd] {
				if slices.Equal(trips, existing.Trips) {
					found = true
					day_types[wd][i].Dates = append(day_types[wd][i].Dates, day)
					break
				}
			}
			if !found {
				day_types[wd] = append(day_types[wd], DayType{[]gtfs.Date{day}, trips})
			}
		}

		sort.Slice(day_types[wd], func(i, j int) bool {
			return len(day_types[wd][i].Dates) > len(day_types[wd][j].Dates)
		})

		for i, _ := range day_types[wd] {
			sort.Slice(day_types[wd][i].Dates, func(a, b int) bool {
				return day_types[wd][i].Dates[a].GetTime().Before(day_types[wd][i].Dates[b].GetTime())
			})
		}
	}

	feed.Services = make(map[string]*gtfs.Service, 0)
	feed.Trips = make(map[string]*gtfs.Trip, 0)
	feed.TripsAddFlds = make(map[string]map[string]string)
	feed.StopTimesAddFlds = make(map[string]map[string]map[int]string)

	// write services
	for wd, _ := range days {
		for _, t := range day_types[wd] {
			weeknums := make([]int, 0)
			for _, d := range t.Dates {
				_, weeknum := d.GetTime().ISOWeek()
				weeknums = append(weeknums, weeknum)
			}

			id := sm.DayNames[t.Dates[0].GetTime().Weekday()]

			if len(day_types[wd]) > 1 {
				id += " ("

				for i, _ := range weeknums {
					if i == 0 {
						id += sm.YearWeekName + strconv.Itoa((weeknums[i]))
						continue
					}

					if weeknums[i] == weeknums[i-1]+1 {
						if id[len(id)-1] != '-' {
							id += "-"
						}
					} else {
						if id[len(id)-1] == '-' {
							id += strconv.Itoa(weeknums[i-1]) + "," + strconv.Itoa((weeknums[i]))
						} else {
							id += "," + strconv.Itoa((weeknums[i]))
						}
					}
				}

				if id[len(id)-1] == '-' {
					id += strconv.Itoa(weeknums[len(weeknums)-1]) + ")"
				} else {
					id += ")"
				}
			}

			exceptions := make(map[gtfs.Date]bool)
			for _, d := range t.Dates {
				exceptions[d] = true
			}

			feed.Services[id] = gtfs.EmptyService()
			feed.Services[id].SetId(id)
			feed.Services[id].SetExceptions(exceptions)

			for _, trip := range t.Trips {
				newt := *trip
				newt.Id = newt.Id + ":" + id
				newt.Service = feed.Services[id]
				newt.StopTimes = append(gtfs.StopTimes{}, trip.StopTimes...)
				feed.Trips[newt.Id] = &newt
			}
		}
	}

	fmt.Fprintf(os.Stdout, "done. (created %d calendar_dates.txt entries, %d monday, %d tuesday, %d wednesday, %d thursday, %d friday, %d saturday, %d sunday types)\n", len(feed.Services), len(day_types[1]), len(day_types[2]), len(day_types[3]), len(day_types[4]), len(day_types[5]), len(day_types[6]), len(day_types[0]))
}
