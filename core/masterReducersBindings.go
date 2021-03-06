package core

import (
	"sort"
	"strconv"
)

/*
module of functions supporting Reducers Workers Istances placement into Workers network
with the target of exploiting data locality to minimize traffic cost of routing intermediate token (MAP() out) to reducers
Costraint of LoadBalacing and Fault Tollerant will be considered during functions
*/
type ReducersDataRouteCosts struct {
	//list of WorkerMapper traffic costs to reducersID of intermediate data
	//for each dest reducerID(K) the cost(V) is the cumulation of the whole expected traffic to itself from a worker
	//reflect the distribuition among mappers of intermediate MAP data to route to reducers
	TrafficCostsReducersDict map[int]map[int]int //WorkerID --> ReducerID --> Cumulative Data Ammount To Route
}
type TrafficCostRecord struct {
	//traffic cost of data to route from  Mappers on a Worker to a Reducer
	//reflect an edge in a bipartite graph (G) weighted on edges reflecting data routing cost between mappers->reducers
	RoutingCost int
	WorkerID    int
	ReducerID   int
}
type ReducersRouteInfos struct {
	DataRoutingCosts           ReducersDataRouteCosts
	ExpectedReduceCallsMappers map[int]map[int]int //for each reducer-> expected calls from mapper (in charge for ChunkID)
	//reducer->ChunkID (mapper work) ->expected calls reduce()
}

func ReducersBindingsLocallityAwareEuristic(reducersIdsTrafficIN ReducersDataRouteCosts, workers *WorkersKinds) map[int]int {
	/*
		Quick Euristic to select placement of Reducers Workers Istances exploiting intermediate Tokens data locality
		minimizing traffic cost of routing data  to reducers will be produced the bindings of reducersID->Address (worker node placement)
		Basic Algo: extract a list of costs for each hash binding to a reducerID
					sort decreasingly by costs
					*select first ISTANCES_NUM_REDUCE records to avoid in REDUCE route phase
		(Graph theory simmetry: assuming a bipartite graph (G) weighted on edges reflecting data routing cost between mappers->reducers
			will be "contracted" first ISTANCES_NUM_REDUCE edges more expensive finding a partition of G )
	*/
	reducersTrafficsCostListSorted := extractCostsListSorted(reducersIdsTrafficIN)
	reducersBindings := make(map[int]int)                                          //final binding of reducerID -> actual worker id for placement
	workerHostingReducers := make(map[int]int)                                     //for each worker used in contraction--> number of contraction on it (hosted reducers)
	maxContractions := Config.ISTANCES_NUM_REDUCE - len(workers.WorkersOnlyReduce) //max Num of reducers contractions
	contractedR := 0
	for i := 0; i < len(reducersTrafficsCostListSorted) && contractedR < maxContractions; i++ {
		record := reducersTrafficsCostListSorted[i]
		_, contractedReducer := reducersBindings[record.ReducerID]
		if !contractedReducer { //NOT ALREADY CONTRACTED THE REDUCER
			workerNode := GetWorker(record.WorkerID, workers, true)
			_, neverContracted := workerHostingReducers[record.WorkerID]
			if neverContracted || workerHostingReducers[record.WorkerID] <= Config.MAX_REDUCERS_PER_WORKER { //NOT TOO MUCH CONTRACTION ON SAME WORKER
				reducersBindings[record.ReducerID] = workerNode.Id //CONTRACT  edge
				println("contracted ", record.ReducerID, "-->", workerNode.Id)
				contractedR++
				workerHostingReducers[record.WorkerID]++
			}
		}
	}
	onlyReducerIndx := 0 //index of worker (type only reduce) in last reducer placement
	for rid := 0; rid < Config.ISTANCES_NUM_REDUCE && onlyReducerIndx < len(workers.WorkersOnlyReduce); rid++ {
		_, contractedReducer := reducersBindings[rid]
		if !contractedReducer {
			reducersBindings[rid] = workers.WorkersOnlyReduce[onlyReducerIndx].Id
			onlyReducerIndx++
		}
	}
	if len(reducersBindings) < Config.ISTANCES_NUM_REDUCE { //TODO ASSERTION CHECK

		panic("reducers placement error" + strconv.Itoa(len(reducersBindings)) + "\t" + strconv.Itoa(len(reducersTrafficsCostListSorted)))
	}

	return reducersBindings
}

func extractCostsListSorted(trafficCosts ReducersDataRouteCosts) []TrafficCostRecord {
	//build list of traffics costs
	trafficCostsRecords := make([]TrafficCostRecord, DictsNestedCumulativeSize(trafficCosts.TrafficCostsReducersDict))
	for mapperID, reducerRoutCosts := range trafficCosts.TrafficCostsReducersDict {
		for reducerID, routingCost := range reducerRoutCosts {
			trafficCostsRecords = append(trafficCostsRecords, TrafficCostRecord{routingCost, mapperID, reducerID})
		}
	}
	//sort list by cost
	routingCostsSorter := RoutingCostsSorter{trafficCostsRecords}
	sort.Sort(sort.Reverse(routingCostsSorter))

	return trafficCostsRecords

}

//TODO PLI script wrap di formulazione su appunti :==))))
