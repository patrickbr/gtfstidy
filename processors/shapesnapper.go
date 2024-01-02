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
	"math"
	"os"
)

// ShapeMinimizer minimizes shapes.
type ShapeSnapper struct {
	MaxDist   float64
	mercs     map[*gtfs.Shape][][]float64
	stopMercs map[*gtfs.Stop][2]float64
}

// Run this ShapeMinimizer on some feed
func (sm ShapeSnapper) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Snapping stop points to shapes... ")

	orign := len(feed.Stops)

	// build projection cache
	sm.mercs = make(map[*gtfs.Shape][][]float64)
	sm.stopMercs = make(map[*gtfs.Stop][2]float64)

	for _, s := range feed.Shapes {
		for _, p := range s.Points {
			x, y := latLngToWebMerc(p.Lat, p.Lon)
			sm.mercs[s] = append(sm.mercs[s], []float64{x, y})
		}
	}

	for _, s := range feed.Stops {
		x, y := latLngToWebMerc(s.Lat, s.Lon)
		sm.stopMercs[s] = [2]float64{x, y}
	}

	for _, t := range feed.Trips {
		if t.Shape == nil {
			continue
		}

		for i, st := range t.StopTimes {
			snaplat, snaplon := webMercToLatLng(sm.snapTo(st.Stop(), st.Shape_dist_traveled(), t.Shape))
			d := haversineApprox(float64(snaplat), float64(snaplon), float64(st.Stop().Lat), float64(st.Stop().Lon))

			if d > sm.MaxDist {
				newId := sm.freeStopId(feed, "#"+st.Stop().Id)

				newStop := gtfs.Stop{
					Id:                  newId,
					Code:                st.Stop().Code,
					Name:                st.Stop().Name,
					Desc:                st.Stop().Desc,
					Lat:                 snaplat,
					Lon:                 snaplon,
					Location_type:       st.Stop().Location_type,
					Wheelchair_boarding: st.Stop().Wheelchair_boarding,
					Zone_id:             st.Stop().Zone_id,
					Url:                 st.Stop().Url,
					Parent_station:      st.Stop().Parent_station,
					Translations:        st.Stop().Translations,
					Level:               st.Stop().Level,
					Platform_code:       st.Stop().Platform_code,
					Timezone:            st.Stop().Timezone,
				}

				feed.Stops[newId] = &newStop
				t.StopTimes[i].SetStop(&newStop)
			}
		}
	}

	fmt.Fprintf(os.Stdout, "done. (+%d stop points [+%.2f%%])\n",
		len(feed.Stops)-orign,
		100.0*float64(len(feed.Stops)-orign)/(float64(orign)+0.001))
}

func (sm *ShapeSnapper) snapTo(stop *gtfs.Stop, distT float32, shape *gtfs.Shape) (float64, float64) {
	shp := sm.mercs[shape]

	if float64(distT) != math.NaN() {
		for i := 1; i < len(shape.Points); i++ {
			if shape.Points[i].Dist_traveled <= distT && i < len(shape.Points) - 1 && shape.Points[i+1].Dist_traveled >= distT {
				d := (distT - shape.Points[i].Dist_traveled) / (shape.Points[i + 1].Dist_traveled - shape.Points[i].Dist_traveled)

				dx := shp[i+1][0] - shp[i][0]
				dy := shp[i+1][1] - shp[i][1]

				x := shp[i][0] + dx*float64(d)
				y := shp[i][1] + dy*float64(d)

				return x, y
			}
		}
	}

	minDist := math.Inf(1)
	minsx := 0.0
	minsy := 0.0

	px := sm.stopMercs[stop][0]
	py := sm.stopMercs[stop][1]

	for i := 1; i < len(shp); i++ {
		sx, sy := snapTo(px, py, shp[i-1][0], shp[i-1][1], shp[i][0], shp[i][1])
		dist := dist(px, py, sx, sy)
		if dist < minDist {
			minsx = sx
			minsy = sy
			minDist = dist
		}
	}

	return minsx, minsy
}

// get a free stop id with the given suffix
func (sm *ShapeSnapper) freeStopId(feed *gtfsparser.Feed, suffix string) string {
	idc := uint(0)
	for idc < ^uint(0) {
		idc += 1
		sid := fmt.Sprint(idc) + suffix
		if _, ok := feed.Stops[sid]; !ok {
			return sid
		}
	}
	panic(errors.New("Ran out of free stop ids."))
}
