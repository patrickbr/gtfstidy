// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
	"github.com/patrickbr/gtfstidy/processors"
	"github.com/patrickbr/gtfswriter"
	flag "github.com/spf13/pflag"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

func parseDate(str string) gtfs.Date {
	var day, month, year int
	var e error
	if len(str) < 8 {
		e = fmt.Errorf("only has %d characters, expected 8", len(str))
	}
	if e == nil {
		day, e = strconv.Atoi(str[6:8])
	}
	if e == nil {
		month, e = strconv.Atoi(str[4:6])
	}
	if e == nil {
		year, e = strconv.Atoi(str[0:4])
	}

	if e != nil {
		panic(fmt.Errorf("Expected YYYYMMDD date, found '%s' (%s)", str, e.Error()))
	}

	return gtfs.Date{Day: int8(day), Month: int8(month), Year: int16(year)}
}

func parseCoords(s string) ([][]float64, error) {
	coords := strings.Split(s, ",")

	if len(coords)%2 != 0 {
		return nil, errors.New("Uneven number of coordinates")
	}

	ret := make([][]float64, 0)
	for i := 0; i < len(coords)/2; i++ {
		var x, y float64
		var err error
		y, err = strconv.ParseFloat(strings.Trim(coords[i*2], "\n "), 64)
		if err == nil {
			x, err = strconv.ParseFloat(strings.Trim(coords[i*2+1], "\n "), 64)
		}

		if err != nil {
			return nil, err
		}

		coord := make([]float64, 2)
		coord[0], coord[1] = x, y
		ret = append(ret, coord)
	}
	return ret, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "gtfstidy - (C) 2016-2021 by P. Brosi <info@patrickbrosi.de>\n\nUsage:\n\n  %s [<options>] [-o <outputfile>] <input GTFS>\n\nAllowed options:\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	polys := make([][][]float64, 0)

	var bboxStrings []string
	var polygonStrings []string
	var polygonFiles []string

	onlyValidate := flag.BoolP("validation-mode", "v", false, "only validate the feed, no processors will be called")

	outputPath := flag.StringP("output", "o", "gtfs-out", "gtfs output directory or zip file (must end with .zip)")

	startDateFilter := flag.StringP("date-start", "", "", "start date filter, as YYYYMMDD")
	endDateFilter := flag.StringP("date-end", "", "", "end date filter, as YYYYMMDD")

	fixShortHand := flag.BoolP("fix", "", false, "shorthand for -eDnz -p '-'")
	compressShortHand := flag.BoolP("compress", "", false, "shorthand for -OSRCcIAP")
	minimizeShortHand := flag.BoolP("Compress", "", false, "shorthand for -OSRCcIAPdT --red-trips-fuzzy, like --compress, but additionally compress stop times into frequencies, use fuzzy matching for redundant trip removal and use dense character ids. The latter destroys any existing external references (like in GTFS realtime streams)")
	mergeShortHand := flag.BoolP("merge", "", false, "shorthand for -ARPICO")
	fuzzyMergeShortHand := flag.BoolP("Merge", "", false, "shorthand for -EARPICO --red-trips-fuzzy")

	useDefaultValuesOnError := flag.BoolP("default-on-errs", "e", false, "if non-required fields have errors, fall back to the default values")
	fixZip := flag.BoolP("fix-zip", "z", false, "try to fix some errors in the ZIP file directory hierarchy")
	emptyStrRepl := flag.StringP("empty-str-repl", "p", "", "string to use if a non-critical required string field is empty (like stop_name, agency_name, ...)")
	dropErroneousEntities := flag.BoolP("drop-errs", "D", false, "drop erroneous entries from feed")
	checkNullCoords := flag.BoolP("check-null-coords", "n", false, "check for (0, 0) coordinates")

	keepStationIds := flag.BoolP("keep-station-ids", "", false, "keep station IDs during ID minimization")
	useOrphanDeleter := flag.BoolP("delete-orphans", "O", false, "remove entities that are not referenced anywhere")
	useShapeMinimizer := flag.BoolP("min-shapes", "s", false, "minimize shapes (using Douglas-Peucker)")
	useShapeRemeasurer := flag.BoolP("remeasure-shapes", "m", false, "remeasure shapes (filling measurement-holes)")
	useRedShapeRemover := flag.BoolP("remove-red-shapes", "S", false, "remove shape duplicates")
	useRedRouteMinimizer := flag.BoolP("remove-red-routes", "R", false, "remove route duplicates")
	useRedServiceMinimizer := flag.BoolP("remove-red-services", "C", false, "remove duplicate services in calendar.txt and calendar_dates.txt")
	useIDMinimizerNum := flag.BoolP("minimize-ids-num", "i", false, "minimize IDs using numerical IDs (e.g. 144, 145, 146...)")
	useIDMinimizerChar := flag.BoolP("minimize-ids-char", "d", false, "minimize IDs using character IDs (e.g. abc, abd, abe, abf...)")
	useServiceMinimizer := flag.BoolP("minimize-services", "c", false, "minimize services by searching for the optimal exception/range coverage")
	useFrequencyMinimizer := flag.BoolP("minimize-stoptimes", "T", false, "search for frequency patterns in explicit trips and combine them, using a CAP approach")
	useCalDatesRemover := flag.BoolP("remove-cal-dates", "", false, "don't use calendar_dates.txt")
	explicitCals := flag.BoolP("explicit-calendar", "", false, "add calendar.txt entry for every service, even irregular ones")
	keepColOrder := flag.BoolP("keep-col-order", "", false, "keep the original column ordering of the input feed")
	keepFields := flag.BoolP("keep-additional-fields", "F", false, "keep all non-GTFS fields from the input")
	useRedStopMinimizer := flag.BoolP("remove-red-stops", "P", false, "remove stop and level duplicates")
	useRedTripMinimizer := flag.BoolP("remove-red-trips", "I", false, "remove trip duplicates")
	useRedTripMinimizerFuzzyRoute := flag.BoolP("red-trips-fuzzy", "", false, "only check MOT of routes for trip duplicate removal")
	useRedAgencyMinimizer := flag.BoolP("remove-red-agencies", "A", false, "remove agency duplicates")
	useStopReclusterer := flag.BoolP("recluster-stops", "E", false, "recluster stops")
	dropShapes := flag.BoolP("drop-shapes", "", false, "drop shapes")
	flag.StringArrayVar(&bboxStrings, "bounding-box", []string{}, "bounding box filter, as comma separated latitude,longitude pairs (multiple boxes allowed by defining --bounding-box multiple times)")
	flag.StringArrayVar(&polygonStrings, "polygon", []string{}, "polygon filter, as comma separated latitude,longitude pairs (multiple polygons allowed by defining --polygon multiple times)")
	flag.StringArrayVar(&polygonFiles, "polygon-file", []string{}, "polygon filter, as a file containing comma separated latitude,longitude pairs (multiple polygons allowed by defining --polygon-file multiple times)")
	showWarnings := flag.BoolP("show-warnings", "W", false, "show warnings")
	help := flag.BoolP("help", "?", false, "this message")

	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	gtfsPaths := flag.Args()

	if len(gtfsPaths) == 0 {
		fmt.Fprintln(os.Stderr, "No GTFS location specified, see --help")
		os.Exit(1)
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "Error:", r)
		}
	}()

	startDate := gtfs.Date{Day: 0, Month: 0, Year: 0}
	endDate := gtfs.Date{Day: 0, Month: 0, Year: 0}

	if len(*startDateFilter) > 0 {
		startDate = parseDate(*startDateFilter)
	}

	if len(*endDateFilter) > 0 {
		endDate = parseDate(*endDateFilter)
	}

	if *fixShortHand {
		*useDefaultValuesOnError = true
		*dropErroneousEntities = true
		*checkNullCoords = true
		*fixZip = true
		*emptyStrRepl = "-"
	}

	if *fuzzyMergeShortHand {
		*mergeShortHand = true
		*useRedTripMinimizerFuzzyRoute = true
		*useStopReclusterer = true
	}

	if *mergeShortHand {
		*useServiceMinimizer = true
		*useRedServiceMinimizer = true
		*useRedTripMinimizer = true
		*useRedAgencyMinimizer = true
		*useRedStopMinimizer = true
		*useRedRouteMinimizer = true
		*useOrphanDeleter = true
	}

	if *minimizeShortHand {
		*compressShortHand = true
		*useIDMinimizerChar = true
		*useFrequencyMinimizer = true
		*useRedTripMinimizerFuzzyRoute = true
	}

	if *compressShortHand {
		*useOrphanDeleter = true
		*useShapeMinimizer = true
		*useRedShapeRemover = true
		*useRedRouteMinimizer = true
		*useRedServiceMinimizer = true
		*useRedStopMinimizer = true
		*useServiceMinimizer = true
		*useRedTripMinimizer = true
		*useRedAgencyMinimizer = true
	}

	for _, polyFile := range polygonFiles {
		bytes, err := ioutil.ReadFile(polyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nCould not parse polygon filter file: ")
			fmt.Fprintf(os.Stderr, err.Error()+".\n")
			os.Exit(1)
		}

		polygonStrings = append(polygonStrings, string(bytes))
	}

	for _, polyString := range polygonStrings {
		poly := make([][]float64, 0)

		if len(polyString) > 0 {
			var err error
			poly, err = parseCoords(polyString)

			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCould not parse polygon filter: ")
				fmt.Fprintf(os.Stderr, err.Error()+".\n")
				os.Exit(1)
			}
		}

		// ensure polygon is closed
		if len(poly) > 1 && (poly[0][0] != poly[len(poly)-1][0] || poly[0][0] != poly[len(poly)-1][0]) {
			poly = append(poly, make([]float64, 2))
			poly[len(poly)-1][0], poly[len(poly)-1][1] = poly[0][0], poly[0][1]
		}

		polys = append(polys, poly)
	}

	for _, bboxString := range bboxStrings {
		bbox := make([][]float64, 0)
		bboxString = strings.Trim(bboxString, " ")

		if len(bboxString) > 0 {
			var err error
			bbox, err = parseCoords(bboxString)

			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCould not parse bounding box filter: ")
				fmt.Fprintf(os.Stderr, err.Error()+".\n")
				os.Exit(1)
			}
		}

		if len(bbox) == 2 {
			poly := make([][]float64, 4)

			poly[0] = make([]float64, 2)
			poly[1] = make([]float64, 2)
			poly[2] = make([]float64, 2)
			poly[3] = make([]float64, 2)

			poly[0][0], poly[0][1] = bbox[0][0], bbox[0][1]
			poly[1][0], poly[1][1] = bbox[0][0], bbox[1][1]
			poly[2][0], poly[2][1] = bbox[1][0], bbox[1][1]
			poly[3][0], poly[3][1] = bbox[1][0], bbox[0][1]

			// ensure polygon is closed
			if len(poly) > 1 && (poly[0][0] != poly[len(poly)-1][0] || poly[0][0] != poly[len(poly)-1][0]) {
				poly = append(poly, make([]float64, 2))
				poly[len(poly)-1][0], poly[len(poly)-1][1] = poly[0][0], poly[0][1]
			}

			polys = append(polys, poly)
		}
	}

	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: *onlyValidate, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false, PolygonFilter: polys}
	opts.DropErroneous = *dropErroneousEntities && !*onlyValidate
	opts.UseDefValueOnError = *useDefaultValuesOnError && !*onlyValidate
	opts.CheckNullCoordinates = *checkNullCoords
	opts.EmptyStringRepl = *emptyStrRepl
	opts.ZipFix = *fixZip
	opts.ShowWarnings = *showWarnings
	opts.DropShapes = *dropShapes
	opts.KeepAddFlds = *keepFields
	opts.DateFilterStart = startDate
	opts.DateFilterEnd = endDate
	feed.SetParseOpts(opts)

	var e error

	if *onlyValidate {
		for _, gtfsPath := range gtfsPaths {
			locFeed := gtfsparser.NewFeed()
			locFeed.SetParseOpts(opts)
			fmt.Fprintf(os.Stdout, "Parsing GTFS feed in '%s' ...", gtfsPath)
			if opts.ShowWarnings {
				fmt.Fprintf(os.Stdout, "\n")
			}
			e = locFeed.Parse(gtfsPath)

			if e != nil {
				fmt.Fprintf(os.Stderr, "\nError while parsing GTFS feed:\n")
				fmt.Fprintln(os.Stderr, e.Error())
				os.Exit(1)
			}
			if opts.ShowWarnings {
				fmt.Fprintf(os.Stdout, "... done.\n")
			} else {
				fmt.Fprintf(os.Stdout, " done.\n")
			}
		}
		fmt.Fprintln(os.Stdout, "No errors.")
		os.Exit(0)
	}

	prefixes := make(map[string]bool, 0)

	for i, gtfsPath := range gtfsPaths {
		fmt.Fprintf(os.Stdout, "Parsing GTFS feed in '%s' ...", gtfsPath)
		if opts.ShowWarnings {
			fmt.Fprintf(os.Stdout, "\n")
		}
		if len(gtfsPaths) > 1 {
			prefix := strconv.FormatInt(int64(i), 10) + "::"
			prefixes[prefix] = true
			e = feed.PrefixParse(gtfsPath, prefix)
		} else {
			e = feed.Parse(gtfsPath)
		}
		if e != nil {
			break
		}

		if opts.DropErroneous {
			s := feed.ErrorStats
			if opts.ShowWarnings {
				fmt.Fprintf(os.Stdout, "... done.")
			} else {
				fmt.Fprintf(os.Stdout, " done.")
			}
			fmt.Fprintf(os.Stdout, " (%d trips [%.2f%%], %d stops [%.2f%%], %d shapes [%.2f%%], %d services [%.2f%%], %d routes [%.2f%%], %d agencies [%.2f%%], %d transfers [%.2f%%], %d pathways [%.2f%%], %d levels [%.2f%%], %d fare attributes [%.2f%%] dropped due to errors.",
				s.DroppedTrips,
				100.0*float64(s.DroppedTrips)/(float64(s.DroppedTrips+len(feed.Trips))+0.001),
				s.DroppedStops,
				100.0*float64(s.DroppedStops)/(float64(s.DroppedStops+len(feed.Stops))+0.001),
				s.DroppedShapes,
				100.0*float64(s.DroppedShapes)/(float64(s.DroppedShapes+feed.NumShpPoints)+0.001),
				s.DroppedServices,
				100.0*float64(s.DroppedServices)/(float64(s.DroppedServices+len(feed.Services))+0.001),
				s.DroppedRoutes,
				100.0*float64(s.DroppedRoutes)/(float64(s.DroppedRoutes+len(feed.Routes))+0.001),
				s.DroppedAgencies,
				100.0*float64(s.DroppedAgencies)/(float64(s.DroppedAgencies+len(feed.Agencies))+0.001),
				s.DroppedTransfers,
				100.0*float64(s.DroppedTransfers)/(float64(s.DroppedTransfers+len(feed.Transfers))+0.001),
				s.DroppedPathways,
				100.0*float64(s.DroppedPathways)/(float64(s.DroppedPathways+len(feed.Pathways))+0.001),
				s.DroppedLevels,
				100.0*float64(s.DroppedLevels)/(float64(s.DroppedLevels+len(feed.Levels))+0.001),
				s.DroppedFareAttributes,
				100.0*float64(s.DroppedFareAttributes)/(float64(s.DroppedFareAttributes+len(feed.FareAttributes))+0.001))
			if !opts.ShowWarnings {
				fmt.Fprintf(os.Stdout, " Use -W to display them.")
			}
			fmt.Print(")\n")
		} else {
			fmt.Fprintf(os.Stdout, " done.\n")
		}
	}

	if e != nil {
		fmt.Fprintf(os.Stderr, "\nError while parsing GTFS feed:\n")
		fmt.Fprintln(os.Stderr, e.Error())
		fmt.Fprintln(os.Stdout, "\nYou may want to try running gtfstidy with --fix for error fixing / skipping. See --help for details.")
		os.Exit(1)
	} else {
		minzers := make([]processors.Processor, 0)

		if *useOrphanDeleter {
			minzers = append(minzers, processors.OrphanRemover{})
		}

		if *useRedAgencyMinimizer {
			minzers = append(minzers, processors.AgencyDuplicateRemover{})
		}

		if *useRedStopMinimizer {
			minzers = append(minzers, processors.StopDuplicateRemover{
				DistThresholdStop:    2.0,
				DistThresholdStation: 50,
			})
		}

		if *useStopReclusterer {
			minzers = append(minzers, processors.StopReclusterer{
				DistThreshold:     75,
				NameSimiThreshold: 0.55,
				GridCellSize:      10000,
			})
		}

		if *useShapeRemeasurer || *useShapeMinimizer || *useRedShapeRemover {
			minzers = append(minzers, processors.ShapeRemeasurer{})
		}

		if *useShapeMinimizer {
			minzers = append(minzers, processors.ShapeMinimizer{Epsilon: 1.0})
		}

		if *useRedShapeRemover {
			minzers = append(minzers, processors.ShapeDuplicateRemover{MaxEqDist: 10.0})
		}

		if *useRedRouteMinimizer {
			minzers = append(minzers, processors.RouteDuplicateRemover{})
		}

		if *useRedServiceMinimizer {
			minzers = append(minzers, processors.ServiceDuplicateRemover{})
		}

		if *useRedTripMinimizer {
			// to convert calendar_dates based services into regular calendar.txt services
			// before concatenating equivalent trips
			if *useServiceMinimizer {
				minzers = append(minzers, processors.ServiceMinimizer{})
			}

			minzers = append(minzers, processors.TripDuplicateRemover{Fuzzy: *useRedTripMinimizerFuzzyRoute})

			// may have created route and stop orphans
			if *useOrphanDeleter {
				minzers = append(minzers, processors.OrphanRemover{})
			}

			// may have created service duplicates
			if *useRedServiceMinimizer {
				minzers = append(minzers, processors.ServiceDuplicateRemover{})
			}
		}

		if *useServiceMinimizer {
			minzers = append(minzers, processors.ServiceMinimizer{})
		}

		if *useFrequencyMinimizer {
			minzers = append(minzers, processors.FrequencyMinimizer{})
		}

		if *useCalDatesRemover {
			minzers = append(minzers, processors.ServiceCalDatesRem{})
		}

		if *useIDMinimizerNum {
			minzers = append(minzers, processors.IDMinimizer{Base: 10, KeepStations: *keepStationIds})
		} else if *useIDMinimizerChar {
			minzers = append(minzers, processors.IDMinimizer{Base: 36, KeepStations: *keepStationIds})
		}

		// do processing
		for _, m := range minzers {
			m.Run(feed)
		}

		// restore stop IDs, if requested
		if *keepStationIds && len(prefixes) > 0 {
			for id, s := range feed.Stops {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Stops[oldId]; !ok {
							feed.Stops[oldId] = s
							feed.Stops[oldId].Id = oldId

							// update additional fields
							for k, _ := range feed.StopsAddFlds {
								feed.StopsAddFlds[k][oldId] = feed.StopsAddFlds[k][id]
								delete(feed.StopsAddFlds[k], id)
							}

							feed.DeleteStop(id)
						}
						break
					}
				}
			}
		}

		fmt.Fprintf(os.Stdout, "Outputting GTFS feed to '%s'...", *outputPath)

		if _, err := os.Stat(*outputPath); os.IsNotExist(err) {
			if path.Ext(*outputPath) == ".zip" {
				os.Create(*outputPath)
			} else {
				os.Mkdir(*outputPath, os.ModePerm)
			}
		}

		// write feed back to output
		w := gtfswriter.Writer{ZipCompressionLevel: 9, Sorted: true, ExplicitCalendar: *explicitCals, KeepColOrder: *keepColOrder}
		e := w.Write(feed, *outputPath)

		if e != nil {
			fmt.Fprintf(os.Stderr, "\nError while writing GTFS feed in '%s':\n ", *outputPath)
			fmt.Fprintln(os.Stderr, e.Error())
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, " done.\n")
	}
}
