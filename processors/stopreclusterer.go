// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"container/heap"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// A StopCluster contains stops in .Childs which are grouped by stops in
// in .Parents (all stops in .Parents have location_type = 1). If a StopCluster
// contains multiple parents, the best matching parent will be chosen later on
type StopCluster struct {
	Parents []*gtfs.Stop
	Childs  []*gtfs.Stop
}

// Create a new StopCluster from a single stops
func NewStopCluster(stop *gtfs.Stop) *StopCluster {
	cluster := StopCluster{}
	cluster.Parents = make([]*gtfs.Stop, 0)
	cluster.Childs = make([]*gtfs.Stop, 0)

	if stop.Location_type == 1 {
		cluster.Parents = append(cluster.Parents, stop)
	} else {
		cluster.Childs = append(cluster.Childs, stop)
	}

	return &cluster
}

// Merge cluster candidate with cluster id and similarity score [0, 1]
type ClusterCand struct {
	id   int
	simi float32
}

// BySimi implements sort.Interface for []ClusterCand based on
// the similarity (bigger = first)
type BySimi []ClusterCand

func (a BySimi) Len() int      { return len(a) }
func (a BySimi) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a BySimi) Less(i, j int) bool {
	return a[i].simi < a[j].simi
}

// StopReclusterer reclusters stops
type StopReclusterer struct {
	DistThreshold     float64
	NameSimiThreshold float64
	GridCellSize      float64
	splitregex        *regexp.Regexp

	// TF-IDF stuff
	wordscores []float32
	wordmap    map[string]int
	vecs       map[*gtfs.Stop]map[int]float64
	tokens     map[*gtfs.Stop]map[string]int

	idx *StopClusterIdx
}

// Run this StopReclusterer on some feed
func (m StopReclusterer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Reclustering stops... ")

	m.splitregex = regexp.MustCompile(`[^\pL]`)

	clusters := make([]*StopCluster, 0)

	// maps from stops to their parent cluster id
	parentClusterMap := make(map[*gtfs.Stop]int, 0)

	// Init cluster list with empty clusters for each stop with
	// location_type = 1
	for _, s := range feed.Stops {
		if s.Location_type == 1 {
			clusters = append(clusters, NewStopCluster(s))
			parentClusterMap[s] = len(clusters) - 1
		}
	}

	// maps from stops to the cluster map where they appear as a child
	childClusterMap := make(map[*gtfs.Stop]int, 0)

	// iterate over stops which aren't stations (location_type = 1) or
	// boarding areas (location_type = 4), the latter don't have a
	// station as parents
	for _, s := range feed.Stops {
		if s.Location_type == 0 || s.Location_type == 2 || s.Location_type == 3 {
			if s.Parent_station != nil {
				// there already is a cluster with the parent, add it
				clusters[parentClusterMap[s.Parent_station]].Childs = append(clusters[parentClusterMap[s.Parent_station]].Childs, s)
				// and update the child cluster map
				childClusterMap[s] = childClusterMap[s.Parent_station]
			} else {
				// create a new single-element cluster with this stop
				clusters = append(clusters, NewStopCluster(s))
				childClusterMap[s] = len(clusters) - 1
			}
		}
	}

	// special handling for boarding areas, see above
	for _, s := range feed.Stops {
		if s.Location_type == 4 {
			// there must be a parent station of s according to the reference
			if s.Parent_station.Parent_station != nil {
				clusters[parentClusterMap[s.Parent_station.Parent_station]].Childs = append(clusters[parentClusterMap[s.Parent_station.Parent_station]].Childs, s)
			} else {
				clusters[childClusterMap[s.Parent_station]].Childs = append(clusters[childClusterMap[s.Parent_station]].Childs, s)
			}
		}
	}

	// geographical grid for faster merge cluster candidate retrieval
	m.idx = NewStopClusterIdx(clusters, m.GridCellSize, m.GridCellSize)

	// build TF-IDF score from all stops
	m.buildTfIdfScores(feed.Stops)

	// pq maintains clusters sorted by their similarity to the nearest merge candidate
	pq := NewPriorityQueue(len(clusters))
	neighs := make([][]ClusterCand, len(clusters))

	// init the pq from the clusters
	for cId := range clusters {
		neighs[cId] = m.getNearestClusters(cId, clusters)
		sort.Sort(BySimi(neighs[cId]))

		if len(neighs[cId]) == 0 {
			pq.Items[cId] = &Item{
				value:    cId,
				priority: 0,
				index:    cId,
			}
		} else {
			pq.Items[cId] = &Item{
				value:    cId,
				priority: neighs[cId][len(neighs[cId])-1].simi,
				index:    cId,
			}
		}
		pq.Pqids[cId] = cId
	}

	// init the PQ to establish the heap attribute
	heap.Init(&pq)

	// take the top merge candidate from the PQ and merge it until the top candidate
	// has priority < 0.5
	for top := heap.Pop(&pq).(*Item); len(pq.Items) > 0; top = heap.Pop(&pq).(*Item) {
		// we can break, there will only be merge candidates below the threshold from now on
		if top.priority < 0.5 {
			break
		}

		neigh := neighs[top.value][len(neighs[top.value])-1]

		// it might happen that the top neighbor is deleted, if that is the case push
		// again with updated neighbours
		if pq.Pqids[neigh.id] >= len(pq.Items) {
			neighs[top.value] = neighs[top.value][:len(neighs[top.value])-1]

			if len(neighs[top.value]) != 0 {
				top.priority = neighs[top.value][len(neighs[top.value])-1].simi
				heap.Push(&pq, top)
				continue
			}
		}

		// merge clusters
		clusters[neigh.id].Parents = append(clusters[neigh.id].Parents, clusters[top.value].Parents...)
		clusters[neigh.id].Childs = append(clusters[neigh.id].Childs, clusters[top.value].Childs...)

		// clear secondary cluster
		clusters[top.value].Parents = nil
		clusters[top.value].Childs = nil

		// update primary cluster neighbor in each neighbor
		for _, n := range neighs[neigh.id] {
			m.updateNeighIn(n.id, neigh.id, clusters, neighs, &pq)
		}

		// update neighs of primary cluster.
		m.updateNeighs(neigh.id, clusters, neighs, &pq)
	}

	// translate the new cluster into the stop relationship
	newl := 0 // keep count of the new clusters
	for _, cl := range clusters {
		// there might now be empty clusters, skip them
		if len(cl.Childs) == 0 && len(cl.Parents) == 0 {
			continue
		}
		newl++

		if len(cl.Childs) == 1 && len(cl.Parents) == 0 {
			continue
		}

		m.writeCluster(cl, feed)
	}

	fmt.Fprintf(os.Stdout, "done. (-%d clusters) [-%.2f%%]\n", (len(clusters) - newl), 100.0*float64(len(clusters)-newl)/(float64(len(clusters))+0.001))
}

func (m *StopReclusterer) writeCluster(cl *StopCluster, feed *gtfsparser.Feed) {
	var parent *gtfs.Stop

	if len(cl.Childs) > 1 && len(cl.Parents) == 0 {
		parent = m.createParent(cl.Childs, feed)
	} else {
		//  take the parent with the best overall similarity
		parent = nil
		bestsimi := float32(0.0)

		for _, p := range cl.Parents {
			cursimi := float32(0.0)
			for _, c := range cl.Childs {
				cursimi += m.stopSimi(p, c)
			}
			cursimi = cursimi / float32(len(cl.Childs))
			if cursimi >= bestsimi {
				parent = p
				bestsimi = cursimi
			}
		}
	}

	for _, s := range cl.Childs {
		if s.Location_type == 0 || s.Location_type == 2 || s.Location_type == 3 {
			s.Parent_station = parent
		}
	}

	for _, st := range cl.Parents {
		if st == parent {
			continue
		}

		for tk, tv := range feed.Transfers {
			tk_new := tk
			if tk.From_stop == st {
				tk_new.From_stop = parent
			}
			if tk.To_stop == st {
				tk_new.To_stop = parent
			}

			if _, ok := feed.Transfers[tk_new]; !ok {
				feed.Transfers[tk_new] = tv
				delete(feed.Transfers, tk)
			}
		}

		for _, p := range feed.Pathways {
			if p.From_stop == st {
				p.From_stop = parent
			}
			if p.To_stop == st {
				p.To_stop = parent
			}
		}

		feed.DeleteStop(st.Id)
	}
}

// Create a parent for clusters without explicit parent stop
func (m *StopReclusterer) createParent(stops []*gtfs.Stop, feed *gtfsparser.Feed) *gtfs.Stop {
	if len(stops) == 0 {
		return nil
	}
	avgLat := float32(0)
	avgLon := float32(0)

	ret := &gtfs.Stop{}
	ret.Wheelchair_boarding = 0
	ret.Timezone, _ = gtfs.NewTimezone("")
	ret.Url = nil

	for _, st := range stops {
		avgLat += st.Lat
		avgLon += st.Lon

		if ret.Wheelchair_boarding == 0 && st.Wheelchair_boarding != 0 {
			ret.Wheelchair_boarding = st.Wheelchair_boarding
		} else if ret.Wheelchair_boarding != 0 && st.Wheelchair_boarding != ret.Wheelchair_boarding {
			ret.Wheelchair_boarding = 0
		}

		if ret.Url == nil && st.Url != nil {
			ret.Url = st.Url
		} else if ret.Url != nil && st.Url != nil && st.Url.String() != ret.Url.String() {
			ret.Url = nil
		}

		if ret.Timezone.GetTzString() == "" && st.Timezone.GetTzString() != "" {
			ret.Timezone = st.Timezone
		} else if ret.Timezone.GetTzString() != "" && st.Timezone.GetTzString() != ret.Timezone.GetTzString() {
			ret.Timezone, _ = gtfs.NewTimezone("")
		}
	}

	ret.Name = stops[0].Name

	for try := 0; ; try++ {
		if try == 0 {
			ret.Id = "par::" + stops[0].Id
		} else {
			ret.Id = "par" + strconv.Itoa(try) + "::" + stops[0].Id
		}
		if _, ok := feed.Stops[ret.Id]; !ok {
			break
		}
	}

	ret.Desc = ""
	ret.Lat = avgLat / float32(len(stops))
	ret.Lon = avgLon / float32(len(stops))
	ret.Location_type = 1
	ret.Parent_station = nil
	ret.Level = nil

	feed.Stops[ret.Id] = ret

	return ret
}

func (m *StopReclusterer) updateNeighIn(cId int, nId int, clusters []*StopCluster, neighs [][]ClusterCand, pq *PriorityQueue) {
	if pq.Pqids[cId] >= len(pq.Items) {
		// cluster was deleted!
		return
	}

	if len(neighs[cId]) == 0 {
		// cluster has no neighs
		return
	}

	oldbestsimi := neighs[cId][len(neighs[cId])-1].simi

	for i, neigh := range neighs[cId] {
		if neigh.id == nId {
			neighs[cId][i].simi = m.clusterSimi(clusters[cId], clusters[nId])

			// only re-sort if not sorted anymore
			if (i != len(neighs[cId])-1 && neighs[cId][i+1].simi < neighs[cId][i].simi) ||
				(i != 0 && neighs[cId][i-1].simi > neighs[cId][i].simi) {

				sort.Sort(BySimi(neighs[cId]))
			}

			// we only have to update the PQ if the highest simi neighbor has changed
			// this is the case if the new simi is bigger, or if we have changed the last element and it is now
			// smaller
			if neigh.simi > oldbestsimi || (i == len(neighs[cId])-1 && neigh.simi < oldbestsimi) {
				pq.Items[pq.Pqids[cId]].priority = neighs[cId][len(neighs[cId])-1].simi
				heap.Fix(pq, pq.Pqids[cId])
			}
			return
		}
	}
}

func (m *StopReclusterer) updateNeighs(cId int, clusters []*StopCluster, neighs [][]ClusterCand, pq *PriorityQueue) {
	if pq.Pqids[cId] >= len(pq.Items) {
		// cluster was deleted!
		return
	}

	oldbestsimi := float32(0.0)

	if len(neighs[cId]) != 0 {
		oldbestsimi = neighs[cId][len(neighs[cId])-1].simi
	}

	neighs[cId] = m.getNearestClusters(cId, clusters)
	sort.Sort(BySimi(neighs[cId]))

	if len(neighs[cId]) == 0 {
		pq.Items[pq.Pqids[cId]].priority = 0
	} else {
		pq.Items[pq.Pqids[cId]].priority = neighs[cId][len(neighs[cId])-1].simi
	}

	if pq.Items[pq.Pqids[cId]].priority != oldbestsimi {
		heap.Fix(pq, pq.Pqids[cId])
	}
}

func (m *StopReclusterer) getNearestClusters(cId int, clusters []*StopCluster) []ClusterCand {
	ret := make([]ClusterCand, 0)

	// assume a max distortion between mercator coordinate distances and real-world distances of 10
	maxDist := m.DistThreshold * 10.0

	neighs := m.idx.GetNeighbors(cId, clusters[cId], maxDist)

	for ncId := range neighs {
		ret = append(ret, ClusterCand{ncId, m.clusterSimi(clusters[cId], clusters[ncId])})
	}

	return ret
}

func (m *StopReclusterer) clusterSimi(a *StopCluster, b *StopCluster) float32 {
	ret := float32(0.0)
	c := 0
	for _, stA := range a.Childs {
		for _, stB := range b.Childs {
			ret += m.stopSimi(stA, stB)
			c++
		}

		for _, stB := range b.Parents {
			ret += m.stopSimi(stA, stB)
			c++
		}
	}

	for _, stA := range a.Parents {
		for _, stB := range b.Childs {
			ret += m.stopSimi(stA, stB)
			c++
		}

		for _, stB := range b.Parents {
			ret += m.stopSimi(stA, stB)
			c++
		}
	}

	if c == 0 {
		return 0
	}

	return ret / float32(c)
}

func (m *StopReclusterer) stopSimi(a *gtfs.Stop, b *gtfs.Stop) float32 {
	geosimi := 0.5 - 0.5*math.Tanh((distSApprox(a, b)-m.DistThreshold)/(m.DistThreshold*0.25))

	vecA, nTokA := m.getTokenVec(a)

	if nTokA == 0 {
		return float32(geosimi)
	}

	vecB, nTokB := m.getTokenVec(b)

	if nTokB == 0 {
		return float32(geosimi)
	}

	namesimi := cosSimi(vecA, vecB)

	if namesimi > m.NameSimiThreshold { // this is the threshold value
		namesimi = 0.5 + (namesimi-m.NameSimiThreshold)/(2*(1-m.NameSimiThreshold))
	} else {
		namesimi = namesimi / (2 * m.NameSimiThreshold)
	}

	return float32(geosimi * namesimi)
}

func (m *StopReclusterer) buildTfIdfScores(stops map[string]*gtfs.Stop) {
	m.wordmap = make(map[string]int)
	m.vecs = make(map[*gtfs.Stop]map[int]float64)
	m.tokens = make(map[*gtfs.Stop]map[string]int)

	for _, st := range stops {
		tokens := m.tokenize(st.Name)
		dl := 0
		for token := range tokens {
			dl++

			if id, ok := m.wordmap[token]; ok {
				m.wordscores[id] = m.wordscores[id] + 1.0
			} else {
				m.wordscores = append(m.wordscores, 1.0)
				m.wordmap[token] = len(m.wordscores) - 1
			}
		}
	}

	for tid := range m.wordscores {
		m.wordscores[tid] = float32(math.Log(float64(float32(len(stops)) / m.wordscores[tid])))
	}
}

func (m *StopReclusterer) getTokenVec(stop *gtfs.Stop) (map[int]float64, int) {
	if vec, ok := m.vecs[stop]; ok {
		return vec, len(m.tokens[stop])
	}

	tokens := m.tokenize(stop.Name)
	ret := make(map[int]float64, 0)

	for token, count := range tokens {
		id := m.wordmap[token]
		ret[id] = float64(m.wordscores[id] * float32(count))
	}

	m.vecs[stop] = ret
	m.tokens[stop] = tokens

	return ret, len(tokens)
}

func (m *StopReclusterer) tokenize(s string) map[string]int {
	ret := make(map[string]int)
	s = strings.ToUpper(s)
	tokens := m.splitregex.Split(s, -1)
	for _, tok := range tokens {
		if tok == "" {
			continue
		}
		if _, ok := ret[tok]; ok {
			ret[tok] = ret[tok] + 1
		} else {
			ret[tok] = 1
		}
	}
	return ret
}
