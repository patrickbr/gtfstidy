// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"github.com/patrickbr/gtfsparser/gtfs"
	"testing"
)

func TestServiceMinimizer(t *testing.T) {
	/**
	 *
	 */

	proc := ServiceMinimizer{}

	testa := gtfs.Service{
		Id:         "a",
		Daymap:     [7]bool{true, true, true, true, true, true, true},
		Start_date: gtfs.Date{Day: 1, Month: 1, Year: 2017},
		End_date:   gtfs.Date{Day: 1, Month: 2, Year: 2027},
	}

	proc.perfectMinimize(&testa)

	if len(testa.Exceptions) != 0 {
		t.Error(testa.Exceptions)
	}

	/**
	 *
	 */

	testa = gtfs.Service{
		Id:         "a",
		Daymap:     [7]bool{true, true, true, true, true, true, true},
		Start_date: gtfs.Date{Day: 1, Month: 1, Year: 2017},
		End_date:   gtfs.Date{Day: 1, Month: 2, Year: 2027},
		Exceptions: make(map[gtfs.Date]int8, 0),
	}

	testa.Exceptions[gtfs.Date{Day: 1, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 2, Month: 1, Year: 2017}] = 1

	proc.perfectMinimize(&testa)

	if len(testa.Exceptions) != 0 {
		t.Error(testa.Exceptions)
	}

	/**
	 *
	 */

	testa = gtfs.Service{
		Id:         "a",
		Daymap:     [7]bool{false, false, false, false, false, false, false},
		Start_date: gtfs.Date{Day: 2, Month: 1, Year: 2013},
		End_date:   gtfs.Date{Day: 8, Month: 1, Year: 2017},
		Exceptions: make(map[gtfs.Date]int8, 0),
	}

	testa.Exceptions[gtfs.Date{Day: 2, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 3, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 4, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 5, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 6, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 7, Month: 1, Year: 2017}] = 1

	proc.perfectMinimize(&testa)

	if len(testa.Exceptions) != 0 {
		t.Error(testa.Exceptions)
	}

	if testa.Start_date.Day != 2 || testa.Start_date.Month != 1 || testa.Start_date.Year != 2017 {
		t.Error(testa.Start_date)
	}

	if testa.End_date.Day != 7 || testa.End_date.Month != 1 || testa.End_date.Year != 2017 {
		t.Error(testa.End_date)
	}

	/**
	 *
	 */

	testa = gtfs.Service{
		Id:         "a",
		Daymap:     [7]bool{false, false, false, false, false, false, false},
		Start_date: gtfs.Date{Day: 2, Month: 1, Year: 2013},
		End_date:   gtfs.Date{Day: 8, Month: 1, Year: 2017},
		Exceptions: make(map[gtfs.Date]int8, 0),
	}

	testa.Exceptions[gtfs.Date{Day: 3, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 4, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 5, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 6, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 7, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 8, Month: 1, Year: 2017}] = 1

	proc.perfectMinimize(&testa)

	if len(testa.Exceptions) != 0 {
		t.Error(testa.Exceptions)
	}

	if testa.Start_date.Day != 3 || testa.Start_date.Month != 1 || testa.Start_date.Year != 2017 {
		t.Error(testa.Start_date)
	}

	if testa.End_date.Day != 8 || testa.End_date.Month != 1 || testa.End_date.Year != 2017 {
		t.Error(testa.End_date)
	}

	for i := 3; i < 9; i++ {
		d := gtfs.Date{Day: int8(i), Month: 1, Year: 2017}
		if !testa.IsActiveOn(d) {
			t.Error(testa)
		}
	}

	d := gtfs.Date{Day: 2, Month: 1, Year: 2017}
	if testa.IsActiveOn(d) {
		t.Error(testa)
	}

	d = gtfs.Date{Day: 9, Month: 1, Year: 2017}
	if testa.IsActiveOn(d) {
		t.Error(testa)
	}

	/**
	 *
	 */

	testa = gtfs.Service{
		Id:         "a",
		Daymap:     [7]bool{false, true, true, false, true, false, false},
		Start_date: gtfs.Date{Day: 2, Month: 1, Year: 2017},
		End_date:   gtfs.Date{Day: 29, Month: 1, Year: 2017},
		Exceptions: make(map[gtfs.Date]int8, 0),
	}

	testa.Exceptions[gtfs.Date{Day: 30, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 31, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 7, Month: 2, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 14, Month: 2, Year: 2017}] = 1

	proc.perfectMinimize(&testa)

	if testa.Start_date.Day != 2 || testa.Start_date.Month != 1 || testa.Start_date.Year != 2017 {
		t.Error(testa.Start_date)
	}

	if testa.End_date.Day != 31 || testa.End_date.Month != 1 || testa.End_date.Year != 2017 {
		t.Error(testa.End_date)
	}

	if len(testa.Exceptions) != 2 {
		t.Error(testa.Exceptions)
	}

	/**
	 *
	 */

	testa = gtfs.Service{
		Id:         "a",
		Daymap:     [7]bool{false, true, true, false, true, false, false},
		Start_date: gtfs.Date{Day: 2, Month: 1, Year: 2017},
		End_date:   gtfs.Date{Day: 29, Month: 1, Year: 2017},
		Exceptions: make(map[gtfs.Date]int8, 0),
	}

	testa.Exceptions[gtfs.Date{Day: 30, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 31, Month: 1, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 7, Month: 2, Year: 2017}] = 1
	testa.Exceptions[gtfs.Date{Day: 14, Month: 2, Year: 2017}] = 1

	proc.perfectMinimize(&testa)

	if testa.Start_date.Day != 2 || testa.Start_date.Month != 1 || testa.Start_date.Year != 2017 {
		t.Error(testa.Start_date)
	}

	if testa.End_date.Day != 31 || testa.End_date.Month != 1 || testa.End_date.Year != 2017 {
		t.Error(testa.End_date)
	}

	if len(testa.Exceptions) != 2 {
		t.Error(testa.Exceptions)
	}

	/**
	 *
	 */

	testa = gtfs.Service{
		Id:         "a",
		Daymap:     [7]bool{true, true, true, true, true, true, true},
		Start_date: gtfs.Date{Day: 2, Month: 1, Year: 2017},
		End_date:   gtfs.Date{Day: 8, Month: 1, Year: 2017},
		Exceptions: make(map[gtfs.Date]int8, 0),
	}

	testa.Exceptions[gtfs.Date{Day: 3, Month: 1, Year: 2017}] = 2

	proc.perfectMinimize(&testa)

	if testa.Start_date.Day != 2 || testa.Start_date.Month != 1 || testa.Start_date.Year != 2017 {
		t.Error(testa.Start_date)
	}

	if testa.End_date.Day != 8 || testa.End_date.Month != 1 || testa.End_date.Year != 2017 {
		t.Error(testa.End_date)
	}

	if len(testa.Exceptions) != 0 {
		t.Error(testa.Exceptions)
	}

	if testa.Daymap[2] {
		t.Error(testa.Daymap)
	}
}
