[![Go Report Card](https://goreportcard.com/badge/github.com/patrickbr/gtfstidy)](https://goreportcard.com/report/github.com/patrickbr/gtfstidy) [![Go Reference](https://pkg.go.dev/badge/github.com/patrickbr/gtfstidy.svg)](https://pkg.go.dev/github.com/patrickbr/gtfstidy)

# gtfstidy

Tidy (and validate) [GTFS](https://developers.google.com/transit/gtfs/) feeds.

Fixes small inconsistencies, minimizes the overall feed size, and prepares the feed for secure, standard-compliant further use.

Output feeds are **semantically equivalent** to the input feed. In this context, semantical equivalency means that the output feed provides exactly the same trips with exactly the same attributes (routes, stop-times, shapes, agency, fares etc.). In other words, the output feed is equivalent to the input feed from a passenger's perspective.

## 0. Features

* **Clean CSV output.** Only quote string values where needed, use dynamic float precision and remove whitespace. Only output files that are necessary.
* **Default-value error handling.** If non-required fields in the input-feed have errors, fall back to the default value according to the GTFS standard
* **Drop-entities error handling.** If non-fixable errors occur, drop the affected entity (trip, route, stop, etc.).
* **Orphan deletion**. Delete stops, routes, stop times and shapes that aren't referenced anywhere
* **ID minimization**. Replace IDs with dense integer or character IDs
* **Shape minimization**. Minimize shape geometries using the [Douglas-Peucker](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm) algorithm
* **Service minimization**. Minimize services in `calender.txt` and `calender_dates.txt` by searching optimal progression and/or exception covers.
* **Trip/Stop-Time minimization**. Minimize trips and stop-times by analyzing `stop_times.txt` and `frequencies.txt` and searching for optimal frequency covers.
* **Shape remeasurement**. If shape measures (`shape_dist_traveled`) have gaps, try to fill them by interpolating surrounding measurements
* **Duplicate removal**. Safely remove routes, shapes and services that are semantically equivalent to others and combine them into one.
* **Extensive feed validation**. Validation includes for example checking stop time progressions, ID references, ID collisions,  missing required fields, shape measurements, timezone strings, general field value range validity, URLs, mail addresses, ISO language codes, timepoint validity ...
* **Entropy-minimizing heuristics**. Sort lines prior to writing them to keep the entropy of the resulting CSV files small. This often leads to better compression results (see below).

For more features, see the help page (`--help`).

## 1. Installation
    $ go install github.com/patrickbr/gtfstidy@latest

## 2. Usage
Each GTFS processor has to be enabled explicitly. See

    $ gtfstidy --help

for possible options.

You can use

	$ gtfstidy -v <feed>

to do a simple feed validation.

## 3. Example

Process the SFMTA-Feed with all processors enabled:

    $ gtfstidy -SCRmTcdsOeD sanfrancisco.zip

Validate the SFMTA-Feed:

    $ gtfstidy -v sanfrancisco.zip

## 4. Evaluation

All zipped sizes using `zip -9`.

### SFMTA feed

Processed with `-SCRmTcdsOeD`.

| File  | # lines before | size before (B) | | # lines after | size after (B) |
|---|---|---|---|---|---|
| `agency.txt`  | 2  | 159 | | 2 | 153 |
| `calendar_dates.txt`  | 11  | 172  | | 11 | 161 |
| `calendar.txt`  | 4  | 194  | | 4 | 190 |
| `fare_attributes.txt`  | 3  |  109 | | 3 | 107 |
| `fare_rules.txt`  | 83 | 1k  | | 83 |  392 |
| `frequencies.txt` | --- | ---  | | **3,904** | **111k** |
| `routes.txt`  | 83  | 3,2K   | | 83 | 1,9k |
| `shapes.txt`  | 201,831  |  7.4M  | | **19,863** | **620K** |
| `stops.txt`  | 3,556  | 212K   | | 3,540 | 172K |
| `stop_times.txt`  | 1,115,598  | 46M   | | **893,344** | **25M** |
| `trips.txt`  | 28,989  | 1,4M   | | **21,741** | **739K** |

Compression gain:

| File  | processors | size before (B)| | size after (B)|
|---|---|---|---|---|
| `feed.zip`  | `-SCRmTcdsOeD` | 8.8M | | **5.9M** |
| `feed.zip`  | *none* | 8.8M | | 8.5M |

### Switzerland feed

Processed with `-SCRmTcdsOeD`.

| File  | # lines before | size before (B) | | # lines after | size after (B) |
|---|---|---|---|---|---|
| `agency.txt`  | 424  | 40K | | 424 | 33K |
| `calendar_dates.txt`  | 4,387,195  | 113M  | | **455,843** | **6.6M** |
| `calendar.txt`  | 32,822  | 2M  | | **28,873** | **1014K** |
| `fare_attributes.txt`  | --- |  --- | | --- | --- |
| `fare_rules.txt`  | --- | ---  | | --- |  --- |
| `frequencies.txt` | --- | ---  | | **2,023** | **57K** |
| `routes.txt`  | 5,564  | 254K   | | 4,613 | **105K** |
| `shapes.txt`  | ---  | --- | | --- | --- |
| `stops.txt`  | 31,753  | 2.4M  | | 31,753 | **1.4M** |
| `stop_times.txt`  | 11,569,991  | 790M   | | **10,771,471** | **323M** |
| `transfers.txt`  | 21,691  | 846K | | 21,691 | **295K** |
| `trips.txt`  | 1,055,231  | 86M | | **662,771** | **26M** |

Compression gain:

| File  | processors | size before (B)| | size after (B)|
|---|---|---|---|---|
| `feed.zip`  | `-SCRmTcdsOeD` | 80M | | **63M** |
| `feed.zip`  | *none* | 80M | | 77M |

### Prague feed

Processed with `-SCRmTcdsOeD`.

| File  | # lines before | size before (B) | | # lines after | size after (B) |
|---|---|---|---|---|---|
| `agency.txt`  | 21  | 1.7K | | 21 | 1.6K |
| `calendar_dates.txt`  | 1  | 32  | | 8 | 122 |
| `calendar.txt`  | 49  | 1.8K  | | 27 | 972 |
| `fare_attributes.txt`  | ---  |  --- | | --- | --- |
| `fare_rules.txt`  | --- | ---  | | --- |  --- | ? |
| `frequencies.txt` | --- | ---  | | **21,425** | **604K** |
| `routes.txt`  | 434  | 25K   | | 434 | 22K |
| `shapes.txt`  | 784,062  |  22M  | | **578,211** | **15M** |
| `stops.txt`  | 7,401  | 385K   | | 7,401 | 323K |
| `stop_times.txt`  | 1,879,757  | 69M   | | **751,415** | **23M** |
| `trips.txt`  | 87,348  | 3.3M   | | **34,812** | **1.1M** |

Compression gain:

| File  | processors | size before (B)| | size after (B)|
|---|---|---|---|---|
| `feed.zip`  | `-SCRmTcdsOeD` | 16M | | **9.4M** |
| `feed.zip`  | *none* | 16M | | 16M |

## 5. Available processors

There are two classes of processors. Processors with a lowercase flag modify existing entries. Processors with an uppercase flag **delete** existing entries, either because they are duplicates or because they can be combined with other entries.

### ID minimizer

---

IDs are packed into dense integer arrays, either as base 10 or base 36 integers. You should not use this processor if you are referencing entities from outside the static feed (for example, if the IDs are references from a GTFS-realtime feed).

#### Flags

* `-i`/`--minimize-ids-num`: pack IDs into dense base 10 integers
* `-d`/`--minimize-ids-char`: pack IDs into dense base 36 integers

#### Modifies
Every file.


#### Example
##### Before

`routes.txt`

```
route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
AB,DTA,10,Airport - Bullfrog,,3,,,
BFC,DTA,20,Bullfrog - Furnace Creek Resort,,3,,,
STBA,DTA,30,Stagecoach - Airport Shuttle,,3,,,
CITY,DTA,40,City,,3,,,
AAMV,DTA,50,Airport - Amargosa Valley,,3,,,
AAMV2,DTA,50,Airport - Amargosa Valley,,3,,,
```

##### After
`routes.txt`
```
route_id,agency_id,route_short_name,route_long_name,route_type
2,1,20,Bullfrog - Furnace Creek Resort,3
3,1,30,Stagecoach - Airport Shuttle,3
4,1,40,City,3
5,1,50,Airport - Amargosa Valley,3
6,1,50,Airport - Amargosa Valley,3
1,1,10,Airport - Bullfrog,3
```

### Orphan remover

---

Feed is checked for entries that are not referenced anywhere. These entries are removed from the output.

#### Flags
* `-O`: remove entities that are not referenced anywhere

#### Modifies

`trips.txt`, `stops.txt`, `routes.txt`, `calendar_dates.txt`, `calendar.txt`

#### Example

##### Before

`stops.txt`

```
stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,parent_station
META1,Furnace Creek,,36.425288,-117.1333162,,,
META2,Furnace Creek,,36.425288,-117.1333162,,,
FUR_CREEK_RES,Furnace Creek Resort (Demo),,36.425288,-117.133162,,,META1
```

##### After

`stops.txt`

```
stop_id,stop_name,stop_lat,stop_lon,parent_station
META1,Furnace Creek,36.42529,-117.133316,
FUR_CREEK_RES,Furnace Creek Resort (Demo),36.42529,-117.13316,META1
```

### Shape minimizer

---

Minimizes shape geometries using Douglas-Peucker. This processor **implicitly calls the shape remeasurer!** The shape coordinates are projected to web mercator ([EPSG:3857](http://spatialreference.org/ref/sr-org/7483/)) prior to minimization. The ε value for Douglas-Peucker is set to 1.0

#### Flags

* `-s`/`--min-shapes`: minimize shapes (using Douglas-Peucker)

#### Modifies

`shapes.txt`

#### Example

##### Before

`shapes.txt`

```
shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
A_shp,0,0,1,
A_shp,0.6,0.5,2,6.8310
A_shp,1,1,3,15.8765
A_shp,2,1,4
A_shp,3,1,5,36.76
A_shp,3.5,1,6,
B_shp,0,0,1,
B_shp,0.6,0.5,2,6.8310
B_shp,1,1,3,15.8765
B_shp,2,1,4
B_shp,3,1,5,36.76
B_shp,3.5,1,6,
```

##### After

`shapes.txt`

```
shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
A_shp,0,0,1,0
A_shp,0.6,0.5,2,6.831
A_shp,1,1,3,15.8765
A_shp,3.5,1,6,42.910156
B_shp,0,0,1,0
B_shp,0.6,0.5,2,6.831
B_shp,1,1,3,15.8765
B_shp,3.5,1,6,42.910156
```
### Service minimizer

---

Minimizes service ranges in `calendar.txt` and `calendar_dates.txt` by searching for optimal coverages of range entries in `calendar.txt` and exception entries in `calendar_dates.txt`.

#### Flags

* `-c`/`--minimize-services`: minimize services by searching for the optimal exception/range coverage

#### Modifies

`calendar.txt`, `calendar_dates.txt`

#### Example

##### Before

`calendar.txt`

```
(empty)
```

`calendar_dates.txt`

```
service_id,date,exception_type
FULLW,20160814,1
FULLW,20160815,2
FULLW,20160816,1
FULLW,20160817,1
FULLW,20160818,1
FULLW,20160819,1
FULLW,20160820,1
FULLW,20160821,1
```

##### After

`calendar.txt`

```
service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
FULLW,0,1,1,1,1,1,1,20160814,20160821
```
`calendar_dates.txt`
```
(empty)
```

### Trip/Stop-times minimizer

Minimizes stop times in `stop_times.txt` and trips in `trips.txt` by searching for progression (frequency) covers on the stop times. If multiple trips with equivalent attributes (route, shapes etc) and the same relative stop times are found, they are checked for frequency patterns. If a pattern can be found, the trips are combined into a single frequency-based trip (via `frequency.txt`). Existing frequencies in `frequencies.txt` are also optimized and/or combined with entries in `stop_times.txt`.

The algorithm is based on a CAP (Cover by Arithmetic Progression) algorithm proposed by [Hannah Bast and Sabine Storandt](http://ad-publications.informatik.uni-freiburg.de/SIGSPATIAL_frequency_BS_2014.pdf).

#### Flags

* `-T`/`--minimize-stoptimes`: search for frequency patterns in explicit trips and combine them

#### Modifies

`trips.txt`, `stop_times.txt`, `frequencies.txt`

#### Example

##### Before

`stop_times.txt`
```
trip_id,arrival_time,departure_time,stop_id,stop_sequence
AB1a,8:00:00,8:00:00,BEATTY_AIRPORT,1
AB1a,8:10:00,8:15:00,BULLFROG,2
AB1b,8:10:00,8:10:00,BEATTY_AIRPORT,1
AB1b,8:20:00,8:25:00,BULLFROG,2
AB1c,8:20:00,8:20:00,BEATTY_AIRPORT,1
AB1c,8:30:00,8:35:00,BULLFROG,2
AB1d,8:30:00,8:30:00,BEATTY_AIRPORT,1
AB1d,8:40:00,8:45:00,BULLFROG,2
```
`trips.txt`
```
route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id
AB,FULLW,AB1a,to Bullfrog,0,1,A_shp
AB,FULLW,AB1b,to Bullfrog,0,1,A_shp
AB,FULLW,AB1c,to Bullfrog,0,1,A_shp
AB,FULLW,AB1d,to Bullfrog,0,1,A_shp
```
`frequencies.txt`
```
(empty)
```

##### After

`stop_times.txt`
```
trip_id,arrival_time,departure_time,stop_id,stop_sequence
AB1a,8:00:00,8:00:00,BEATTY_AIRPORT,1
AB1a,8:10:00,8:15:00,BULLFROG,2
```
`trips.txt`
```
route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id
AB,FULLW,AB1a,to Bullfrog,0,1,A_shp
```
`frequencies.txt`
```
trip_id,start_time,end_time,headway_secs,exact_times
AB1a,8:00:00,8:40:00,600,1
```
### Route duplicate remover

---

Removes duplicate routes (routes that have the same attributes and the same fare rules), updates references in `trips.txt` and deletes redundant rules in `fare_rules.txt` as well.

#### Flags

* `-R`/`--remove-red-routes`: remove route duplicates

#### Modifies

`routes.txt`, `trips.txt`, `fare_rules.txt`

#### Example

##### Before

`routes.txt`

```
route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
AB,DTA,10,Airport - Bullfrog,,3,,,
BFC,DTA,20,Bullfrog - Furnace Creek Resort,,3,,,
CFC,DTA,20,Bullfrog - Furnace Creek Resort,,3,,,
```
`fare_rules.txt`
```
fare_id,route_id,origin_id,destination_id,contains_id
p,AB,,,
p,BFC,,,
p,CFC,,,
```

##### After

`routes.txt`
```
route_id,agency_id,route_short_name,route_long_name,route_type
AB,DTA,10,Airport - Bullfrog,3
CFC,DTA,20,Bullfrog - Furnace Creek Resort,3
```

`fare_rules.txt`
```
fare_id,route_id
p,AB
p,CFC
```

### Service duplicate remover

---

Removes duplicate services (services that cover the same set of dates) and updates references.

#### Flags

* `-C`/`--remove-red-services`: remove duplicate services in calendar.txt and calendar_dates.txt

#### Modifies

`calendar_dates.txt`, `calendar.txt`, `trips.txt`

#### Example

##### Before

`calendar_dates.txt`
```
service_id,date,exception_type
B,20160815,1
B,20160816,1
B,20160817,1
B,20160818,1
B,20160819,1
B,20160820,1
```
`calendar.txt`
```
service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
A,1,1,1,1,1,1,0,20160814,20160821
```

##### After

`calendar_dates.txt`
```
(empty)
```
`calendar.txt`
```
service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
A,1,1,1,1,1,1,0,20160814,20160821
```

### Shape duplicate remover

---

Removes duplicate shapes and updates references in `trips.txt`. Shape equality testing is done with a simple heuristic which resembles the [Fréchet-Distance](https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance) but is faster. The check never underestimates the distance between two shapes, but overestimates it for shapes with total distances that are `>>` the max distance. **This processor implicitely calls the shape remeasurer**.

#### Flags
* `-S`/`--remove-red-shapes`: remove shape duplicates

#### Modifies
`shapes.txt`, `trips.txt`

#### Example

##### Before

`shapes.txt`

```
shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
A_shp,0,0,1,0
A_shp,0.6,0.5,2,6.831
A_shp,1,1,3,15.8765
A_shp,2,1,4,26.315065
A_shp,3,1,5,36.76
A_shp,3.5,1,6,42.910156
B_shp,0,0,1,0
B_shp,0.6,0.5,2,6.831
B_shp,1,1,3,15.8765
B_shp,2,1,4,26.315065
B_shp,3,1,5,36.75
B_shp,3.500005,1,6,42.91
```

##### After

`shapes.txt`

```
shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
B_shp,0,0,1,0
B_shp,0.6,0.5,2,6.831
B_shp,1,1,3,15.8765
B_shp,2,1,4,26.315065
B_shp,3,1,5,36.75
B_shp,3.500005,1,6,42.91
```

### Shape remeasurer

---

Remeasures shapes and fills measurement gaps.

#### Flags

* `-m`/`--remeasure-shapes`: remeasure shapes (filling measurement-holes)

#### Modifies

`shapes.txt`

#### Example:

##### Before

`shapes.txt`

```
shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
A_shp,0,0,1,
A_shp,0.6,0.5,2,6.8310
A_shp,1,1,3,15.8765
A_shp,2,1,4
A_shp,3,1,5,36.76
A_shp,3.5,1,6,
```

##### After

`shapes.txt`

```
shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
A_shp,0,0,1,0
A_shp,0.6,0.5,2,6.831
A_shp,1,1,3,15.8765
A_shp,2,1,4,26.315065
A_shp,3,1,5,36.76
A_shp,3.5,1,6,42.910156

```

### Set erroneous values to standard defaults

---

If optional field values of feed entries have errors, this processors sets them to the default values specified in the GTFS standard.

#### Flags

* `-e`/`--default-on-errs`: if non-required fields have errors, fall back to the default values

#### Modifies

Every file, if errors are present.

#### Example:

##### Before

`routes.txt`

```
route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
AB,DTAoopserror,10,Airport - Bullfrog,,3,,,
BFC,DTA,20,Bullfrog - Furnace Creek Resort,,3,,,
STBA,DTA,30,Stagecoach - Airport Shuttle,,3,,,
CITY,DTA,40,City,,3,,,
AAMV,DTA,50,Airport - Amargosa Valley,,3,,,
AAMV2,DTA,50,Airport - Amargosa Valley,,3,,,
```

##### After

`routes.txt`

```
route_id,agency_id,route_short_name,route_long_name,route_type
CITY,DTA,40,City,3
AAMV,DTA,50,Airport - Amargosa Valley,3
AAMV2,DTA,50,Airport - Amargosa Valley,3
AB,,10,Airport - Bullfrog,3
BFC,DTA,20,Bullfrog - Furnace Creek Resort,3
STBA,DTA,30,Stagecoach - Airport Shuttle,3
```

### Drop erroneous entries

---

If feed entries have errors that can't be fixed in any other way (e.g. by `-e`), this processor completely removes them. This cascades through the entire feed: if an erroneous route is deleted, all trips that refer to this route are also deleted, all stop times for trips referencing this route are deleted as well and so on. You effectively get an error-free subset of the input feed.

#### Flags:

* `-D`/`--drop-errs`: drop erroneous entries from feed

#### Modifies:

Every file, if errors are present.

#### Example:

##### Before

`routes.txt`

```
route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
AB,DTAoopserror,10,Airport - Bullfrog,,3,,,
BFC,DTA,20,Bullfrog - Furnace Creek Resort,,3,,,
STBA,DTA,30,Stagecoach - Airport Shuttle,,3,,,
CITY,DTA,40,City,,3,,,
AAMV,DTA,50,Airport - Amargosa Valley,,3,,,
AAMV2,DTA,50,Airport - Amargosa Valley,,3,,,
```

##### After

`routes.txt`

```
route_id,agency_id,route_short_name,route_long_name,route_type
CITY,DTA,40,City,3
AAMV,DTA,50,Airport - Amargosa Valley,3
AAMV2,DTA,50,Airport - Amargosa Valley,3
BFC,DTA,20,Bullfrog - Furnace Creek Resort,3
STBA,DTA,30,Stagecoach - Airport Shuttle,3
```

## 6. Processing order

The order in which feed processors are called is *always the same*, regardless of the flag ordering. The processors are run in this order:

1. Default erroneous values (`-e`)
2. Drop erroneous entries (`-D`)
3. Remove orphans (`-O`)
4. Remeasure shapes (`-m`)
5. Minimize shapes (`-s`)
6. Remove shape duplicates (`-S`)
7. Remove route duplicates (`-R`)
8. Remove service duplicates (`-C`)
9. Minimize services (`-c`)
10. Minimize stop-times/trips (`-T`)
11. Minimize IDs (`-i` or `-d`)

## 7. License

GPL v2, see LICENSE
