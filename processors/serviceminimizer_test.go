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

	testa := gtfs.EmptyService()
	testa.SetId("a")
	testa.SetRawDaymap(255)
	testa.SetStart_date(gtfs.NewDate(1, 1, 2017))
	testa.SetEnd_date(gtfs.NewDate(1, 2, 2017))

	proc.perfectMinimize(testa)

	if len(testa.Exceptions()) != 0 {
		t.Error(testa.Exceptions())
	}

	/**
	 *
	 */

	testa = gtfs.EmptyService()
	testa.SetId("a")
	testa.SetRawDaymap(255)
	testa.SetStart_date(gtfs.NewDate(1, 1, 2017))
	testa.SetEnd_date(gtfs.NewDate(1, 2, 2017))

	testa.Exceptions()[gtfs.NewDate(1, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(2, 1, 2017)] = true

	proc.perfectMinimize(testa)

	if len(testa.Exceptions()) != 0 {
		t.Error(testa.Exceptions())
	}

	/**
	 *
	 */

	testa = gtfs.EmptyService()
	testa.SetId("a")
	testa.SetRawDaymap(0)
	testa.SetStart_date(gtfs.NewDate(2, 1, 2013))
	testa.SetEnd_date(gtfs.NewDate(8, 1, 2017))

	testa.Exceptions()[gtfs.NewDate(2, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(3, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(4, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(5, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(6, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(7, 1, 2017)] = true

	proc.perfectMinimize(testa)

	if len(testa.Exceptions()) != 0 {
		t.Error(testa.Exceptions())
	}

	if testa.Start_date().Day() != 2 || testa.Start_date().Month() != 1 || testa.Start_date().Year() != 2017 {
		t.Error(testa.Start_date())
	}

	if testa.End_date().Day() != 7 || testa.End_date().Month() != 1 || testa.End_date().Year() != 2017 {
		t.Error(testa.End_date())
	}

	/**
	 *
	 */

	testa = gtfs.EmptyService()
	testa.SetId("a")
	testa.SetRawDaymap(0)
	testa.SetStart_date(gtfs.NewDate(2, 1, 2013))
	testa.SetEnd_date(gtfs.NewDate(8, 1, 2017))

	testa.Exceptions()[gtfs.NewDate(3, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(4, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(5, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(6, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(7, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(8, 1, 2017)] = true

	proc.perfectMinimize(testa)

	if len(testa.Exceptions()) != 0 {
		t.Error(testa.Exceptions())
	}

	if testa.Start_date().Day() != 3 || testa.Start_date().Month() != 1 || testa.Start_date().Year() != 2017 {
		t.Error(testa.Start_date())
	}

	if testa.End_date().Day() != 8 || testa.End_date().Month() != 1 || testa.End_date().Year() != 2017 {
		t.Error(testa.End_date())
	}

	for i := 3; i < 9; i++ {
		d := gtfs.NewDate(uint8(i), 1, 2017)
		if !testa.IsActiveOn(d) {
			t.Error(testa)
		}
	}

	d := gtfs.NewDate(2, 1, 2017)
	if testa.IsActiveOn(d) {
		t.Error(testa)
	}

	d = gtfs.NewDate(9, 1, 2017)
	if testa.IsActiveOn(d) {
		t.Error(testa)
	}

	/**
	 *
	 */

	testa = gtfs.EmptyService()
	testa.SetId("a")
	testa.SetDaymap(1, true)
	testa.SetDaymap(2, true)
	testa.SetDaymap(4, true)
	testa.SetStart_date(gtfs.NewDate(2, 1, 2017))
	testa.SetEnd_date(gtfs.NewDate(29, 1, 2017))

	testa.Exceptions()[gtfs.NewDate(30, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(31, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(7, 2, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(14, 2, 2017)] = true

	proc.perfectMinimize(testa)

	if testa.Start_date().Day() != 2 || testa.Start_date().Month() != 1 || testa.Start_date().Year() != 2017 {
		t.Error(testa.Start_date())
	}

	if testa.End_date().Day() != 31 || testa.End_date().Month() != 1 || testa.End_date().Year() != 2017 {
		t.Error(testa.End_date())
	}

	if len(testa.Exceptions()) != 2 {
		t.Error(testa.Exceptions())
	}

	/**
	 *
	 */

	testa = gtfs.EmptyService()
	testa.SetId("a")
	testa.SetDaymap(1, true)
	testa.SetDaymap(2, true)
	testa.SetDaymap(4, true)
	testa.SetStart_date(gtfs.NewDate(2, 1, 2017))
	testa.SetEnd_date(gtfs.NewDate(29, 1, 2017))

	testa.Exceptions()[gtfs.NewDate(30, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(31, 1, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(7, 2, 2017)] = true
	testa.Exceptions()[gtfs.NewDate(14, 2, 2017)] = true

	proc.perfectMinimize(testa)

	if testa.Start_date().Day() != 2 || testa.Start_date().Month() != 1 || testa.Start_date().Year() != 2017 {
		t.Error(testa.Start_date())
	}

	if testa.End_date().Day() != 31 || testa.End_date().Month() != 1 || testa.End_date().Year() != 2017 {
		t.Error(testa.End_date())
	}

	if len(testa.Exceptions()) != 2 {
		t.Error(testa.Exceptions())
	}

	/**
	 *
	 */

	testa = gtfs.EmptyService()
	testa.SetId("a")
	testa.SetRawDaymap(255)
	testa.SetStart_date(gtfs.NewDate(2, 1, 2017))
	testa.SetEnd_date(gtfs.NewDate(8, 1, 2017))

	testa.Exceptions()[gtfs.NewDate(3, 1, 2017)] = false

	proc.perfectMinimize(testa)

	if testa.Start_date().Day() != 2 || testa.Start_date().Month() != 1 || testa.Start_date().Year() != 2017 {
		t.Error(testa.Start_date())
	}

	if testa.End_date().Day() != 8 || testa.End_date().Month() != 1 || testa.End_date().Year() != 2017 {
		t.Error(testa.End_date())
	}

	if len(testa.Exceptions()) != 0 {
		t.Error(testa.Exceptions())
	}

	if testa.Daymap(2) {
		t.Error(testa.Daymap(2))
	}
}
