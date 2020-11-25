// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de

//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfstidy/processors"
	"github.com/patrickbr/gtfswriter"
	flag "github.com/spf13/pflag"
	"os"
	"path"
	"strconv"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "gtfstidy - (C) 2016-2020 by P. Brosi <info@patrickbrosi.de>\n\nUsage:\n\n  %s [<options>] [-o <outputfile>] <input GTFS>\n\nAllowed options:\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	onlyValidate := flag.BoolP("validation-mode", "v", false, "only validate the feed, no processors will be called")

	outputPath := flag.StringP("output", "o", "gtfs-out", "gtfs output directory or zip file (must end with .zip)")

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
	useRedStopMinimizer := flag.BoolP("remove-red-stops", "P", false, "remove stops duplicates")
	useRedTripMinimizer := flag.BoolP("remove-red-trips", "I", false, "remove trip duplicates")
	useRedTripMinimizerFuzzyRoute := flag.BoolP("red-trips-fuzzy", "", false, "only check MOT of routes for trip duplicate removal")
	useRedAgencyMinimizer := flag.BoolP("remove-red-agencies", "A", false, "remove agency duplicates")
	useStopReclusterer := flag.BoolP("recluster-stops", "E", false, "recluster stops")
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

	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: *onlyValidate, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false}
	opts.DropErroneous = *dropErroneousEntities && !*onlyValidate
	opts.UseDefValueOnError = *useDefaultValuesOnError && !*onlyValidate
	opts.CheckNullCoordinates = *checkNullCoords
	opts.EmptyStringRepl = *emptyStrRepl
	opts.ZipFix = *fixZip
	feed.SetParseOpts(opts)

	var e error

	if *onlyValidate {
		for _, gtfsPath := range gtfsPaths {
			locFeed := gtfsparser.NewFeed()
			locFeed.SetParseOpts(opts)
			fmt.Fprintf(os.Stdout, "Parsing GTFS feed in '%s' ...", gtfsPath)
			e = locFeed.Parse(gtfsPath)

			if e != nil {
				fmt.Fprintf(os.Stderr, "\nError while parsing GTFS feed:\n")
				fmt.Fprintln(os.Stderr, e.Error())
				os.Exit(1)
			}
			fmt.Fprintf(os.Stdout, " done.\n")
		}
		fmt.Fprintln(os.Stdout, "No errors.")
		os.Exit(0)
	}

	for i, gtfsPath := range gtfsPaths {
		fmt.Fprintf(os.Stdout, "Parsing GTFS feed in '%s' ...", gtfsPath)
		if len(gtfsPaths) > 1 {
			e = feed.PrefixParse(gtfsPath, strconv.FormatInt(int64(i), 10)+"::")
		} else {
			e = feed.Parse(gtfsPath)
		}
		if e != nil {
			break
		}
		fmt.Fprintf(os.Stdout, " done.\n")
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
			minzers = append(minzers, processors.StopDuplicateRemover{})
		}

		if *useStopReclusterer {
			minzers = append(minzers, processors.StopReclusterer{
				DistThreshold:     75,
				NameSimiThreshold: 0.55,
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
			minzers = append(minzers, processors.TripDuplicateRemover{Fuzzy: *useRedTripMinimizerFuzzyRoute})

			// may have created route and stop orphans
			if *useOrphanDeleter {
				minzers = append(minzers, processors.OrphanRemover{})
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
			minzers = append(minzers, processors.IDMinimizer{Base: 10})
		} else if *useIDMinimizerChar {
			minzers = append(minzers, processors.IDMinimizer{Base: 36})
		}

		// do processing
		for _, m := range minzers {
			m.Run(feed)
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
