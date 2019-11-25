package core

import (
	"../aws_SDK_wrap"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

const WORKER_CHUNKS_INITSIZE_DFLT = 3

type CHUNK string

const ( //WORKERS KINDS
	WORKERS_MAP_REDUCE  = "MAP_REDUCE"
	WORKERS_ONLY_REDUCE = "ONLY_REDUCE"
	WORKERS_BACKUP_W    = "WORKERS_BACKUP"
)

/////////		WORKER STRUCTS		//////
///MASTER SIDE STRUCTS
type WorkersKinds struct { //refs to up&running workers
	WorkersMapReduce  []Worker //mutable worker
	WorkersOnlyReduce []Worker //separated worker for reduce
	WorkersBackup     []Worker //idle workers for backup
}

type Worker struct { //worker istance master side
	Address         string
	PingServicePort int //TODO USEFUL ON LOCAL VERSION ONLY
	Id              int
	State           WorkerStateMasterControl
}
type WorkerStateMasterControl struct {
	//struct for all data that a worker node may have during computation
	//because a worker may or may not become both Mapper and Reducer not all these filds will be used
	ChunksIDs               []int //all chunks located on Worker
	MapIntermediateTokenIDs []int //computed map interm.Tokens from assigned chunk (same id of chunk)
	ReducersHostedIDs       []int
	//WorkerIstances     map[int]WorkerIstanceControl //for each worker istance ID -> control info (port,state,internalData)
	ControlRPCInstance WorkerIstanceControl //chunkService worker node level instance
	Failed             bool
	//backward link it
}

///WORKER ISTANCE SIDE		routine-task-~-container adaptable
type MapperIstanceStateInternal struct {
	IntermediateTokens map[string]int //produced by map, routed to Reducer
	WorkerChunks       *WorkerChunks  //readonly ref to chunks stored in worker that contains this Mapper istance
	ChunkID            int            //assigned ChunkID unique identifier for a MAPPER work result
	DestinationCosts   Map2ReduceRouteCost
}

type ReducerIstanceStateInternal struct {
	IntermediateTokensCumulative map[string]int //used by reduce calls to aggregate intermediate Tokens from the map executions
	CumulativeCalls              map[int]bool   //cumulative number of reduce calls received per chunk ID (for exit condition) (
	AllEnded                     bool
	mutex                        sync.Mutex  //protect cumulative vars from concurrent access
	MasterClient                 *rpc.Client //to report back to master final token share of reducer
	LogicID                      int
	StateChan                    chan uint32 //to propagate at worker level
	Server                       *rpc.Server //to propagate at worker level
	Port                         int
	MasterAddress                string
	REPLAY_ANSWER                int

	//to propagate at worker level
}

type GenericInternalState struct { //worker instance generic internal state ( field to use is discriminable from instance kind filed)
	MapData    MapperIstanceStateInternal
	ReduceData ReducerIstanceStateInternal
}
type WorkerIstanceControl struct { //MASTER SIDE instance
	Id       int
	Port     int
	Kind     int
	IntState int     //internal state of the istance
	Client   *CLIENT //not nil only at master
	//Client   *rpc.Client //not nil only at master

}
type WorkerInstanceInternal struct {
	Kind        int //either MAP or REDUCE
	ListenerRpc net.Listener
	ServerRpc   *rpc.Server
	Port        int
	IntData     GenericInternalState //generic data carried by istance discriminated by kind //TODO REDUNDANT OTHER THAN intState=?
}

//WORKER_ISTANCE_STATES, NB not all istances will execute all these states
const (
	IDLE int = iota
	MAP_EXEC
	WAITING_REDUCERS_ADDRESSES
	REDUCE_EXEC
	FAILED
)

//INIT RPC SERVICE CODES to register different RPC
const (
	CONTROL int = iota //control services
	MAP
	REDUCE
	MASTER
)

type AggregatedIntermediateTokens struct {
	ChunksSouces                 []int
	PerReducerIntermediateTokens []map[string]int
	FlushedIntermediateTokens    []bool //ReducerLogicID-->at least once sent interm.tokens
	MasterOutputCache            Map2ReduceRouteCost
}

///WORKER SIDE STRUCTS
type ReduceOutputCache struct {
	reducerBindings map[int]string
	Errs            []error
}
type Worker_node_internal struct {
	//global worker istance worker node side
	WorkerChunksStore          WorkerChunks //chunks stored in the worker node
	IntermediateDataAggregated AggregatedIntermediateTokens
	MapperInstances            map[int]MapperIstanceStateInternal
	Instances                  map[int]WorkerInstanceInternal //TODO MAKE REDUCE INSTANCES ONLY
	ControlRpcInstance         WorkerInstanceInternal         //worker node main control instace ->chunks&&respawn
	ReducersClients            []*rpc.Client
	Id                         int         //TODO OLD
	StateChan                  chan uint32 //actual worker state for pong
	PingPort                   int
	Downloader                 *aws_SDK_wrap.DOWNLOADER
	MasterAddr                 string
	//////////////// master fault tollerant recovery outputs
	cacheLock   sync.Mutex
	reduceCache ReduceOutputCache
	mapsCache   Map2ReduceRouteCost
	///////// local version chan for internal correct termination
	ExitChan         chan bool
	ReducerInstances []ReducerIstanceStateInternal
}
type WorkerChunks struct { //WORKER NODE LEVEL STRUCT
	//HOLD chunks stored in worker node pretected by a mutex
	Mutex  sync.Mutex    //protect concurrent access to chunks
	Chunks map[int]CHUNK //chunks stored by Id in worker

}

func InitRPCWorkerIstance(initData *GenericInternalState, port int, kind int, workerNodeInt *Worker_node_internal) (error, int) {
	//Create an instance of structs which implements map and reduce interfaces
	//init code used to discriminate witch rpc to activate (see costants)
	//initialization data for the new instance can be provided with parameter initData filling the right field
	//return error or new ID for the created instance
	var err error
	maxId := GetMaxIdWorkerInstances(&workerNodeInt.Instances) //find new unique ID
	newId := maxId + 1
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	if CheckErr(e, false, "listen err on port "+strconv.Itoa(port)) {
		return e, -1
	}
	workerIstanceData := WorkerInstanceInternal{
		Kind:        kind,
		ListenerRpc: l,
		ServerRpc:   rpc.NewServer(),
		Port:        port,
	}
	/*if kind == MAP {
		mapper := new(MapperIstanceStateInternal)
		if initData != nil {
			mapper = &(initData.MapData)
		}
		err = rpcServer.RegisterName("MAP", mapper)
		mapper.WorkerChunks = &workerNodeInt.WorkerChunksStore //link to chunks for mapper
		workerIstanceData.IntData.MapData = MapperIstanceStateInternal(*mapper)
	} else*/if kind == REDUCE {
		reducer := new(ReducerIstanceStateInternal)
		if initData != nil {
			reducer = &(initData.ReduceData)
		}
		err = workerIstanceData.ServerRpc.RegisterName("REDUCE", reducer)
		go workerIstanceData.ServerRpc.Accept(l)
		workerIstanceData.IntData.ReduceData = ReducerIstanceStateInternal(*reducer)
		(*workerNodeInt).Instances[maxId] = workerIstanceData
	} else if kind == CONTROL { //instances for controlRPCs and ChunksService at workerNodeLevel
		//chunk service
		workerNodeInt.WorkerChunksStore.Chunks = make(map[int]CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		err = workerIstanceData.ServerRpc.RegisterName("CONTROL", workerNodeInt)
		workerNodeInt.ControlRpcInstance = workerIstanceData
	} else {
		panic("unrecognized code" + strconv.Itoa(kind))
	}
	if CheckErr(err, false, "Format of service selected  is not correct: ") {
		return err, -1
	}
	//insert the newly created instance
	if kind != CONTROL { //append only standard instances, the control own a special field
		workerNodeInt.Instances[newId] = workerIstanceData
	}
	// Listen for incoming tcp packets on port by specified offset of port base.
	go workerIstanceData.ServerRpc.Accept(l)
	//_:=l.Close()
	//runtime.Goexit()    		//routine end here
	return nil, newId
}

func (workerNodeInt *Worker_node_internal) initLogicWorkerIstance(initData *GenericInternalState, kind int, id *int) *WorkerInstanceInternal {
	/*
		initialize a new logic worker instance inside worker node
		kind used to discriminate instance tipe, initData is eventual initialization data for the new instance to create
		returned the new instance ref and eventual errors
	*/
	maxId := GetMaxIdWorkerInstances(&workerNodeInt.Instances) //find new unique ID
	newId := maxId + 1
	if id != nil { //try to assign id to the new instance if not assigned already
		_, present := workerNodeInt.Instances[*id]
		if present {
			_, _ = fmt.Fprint(os.Stderr, "re instantiation of logic instance on worker")

		}
		newId = *id
	}
	workerIstanceData := WorkerInstanceInternal{
		Kind: kind,
	}
	if kind == MAP {
		mapper := *new(MapperIstanceStateInternal)
		mapper.IntermediateTokens = make(map[string]int)
		if initData != nil {
			mapper = (initData.MapData)
		}
		mapper.WorkerChunks = &workerNodeInt.WorkerChunksStore //link to chunks for mapper
		workerIstanceData.IntData.MapData = mapper
	} else if kind == REDUCE {
		reducer := new(ReducerIstanceStateInternal)
		if initData != nil {
			reducer = &(initData.ReduceData)
		}
		workerIstanceData.IntData.ReduceData = *reducer
	} else if kind == CONTROL { //instances for controlRPCs and ChunksService at workerNodeLevel
		//chunk service
		workerNodeInt.WorkerChunksStore.Chunks = make(map[int]CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		workerNodeInt.ControlRpcInstance = workerIstanceData
	} else {
		panic("unrecognized code" + strconv.Itoa(kind))
	}
	if kind != CONTROL { //append only standard instances, the control own a special field
		workerNodeInt.Instances[newId] = workerIstanceData
	}
	return &workerIstanceData
}

func (workerNode *Worker_node_internal) aggregateIntermediateTokens(mapJobs []int, storeFlag bool) []map[string]int {

	perReducerIntermediateTokens := make([]map[string]int, Config.ISTANCES_NUM_REDUCE)

	for i := 0; i < Config.ISTANCES_NUM_REDUCE; i++ {
		//init aggregation var
		perReducerIntermediateTokens[i] = make(map[string]int)
	}
	aggreagated := 0
	for _, jobID := range mapJobs {
		founded := false
		for chunkID, instance := range workerNode.MapperInstances {
			if chunkID == jobID {
				/// map instace data aggregation requested ...
				for key, value := range instance.IntermediateTokens {
					destReducer := HashKeyReducerSum(key, Config.ISTANCES_NUM_REDUCE)
					perReducerIntermediateTokens[destReducer][key] += value //aggregate token per destination reducer
				}
				founded = true
				aggreagated++
				break
			}
		}
		if !founded {
			println("MISSING" + strconv.Itoa(jobID) + "\t" + strconv.Itoa(workerNode.ControlRpcInstance.Port))
		}
	}
	if aggreagated != len(mapJobs) {
		GenericPrint(mapJobs, "PANIC AT \t"+strconv.Itoa(workerNode.ControlRpcInstance.Port))
		panic(strconv.Itoa(len(mapJobs)) + "vs" + strconv.Itoa(aggreagated) + "vs" + strconv.Itoa(len(workerNode.MapperInstances)))
	}
	GenericPrint(mapJobs, "aggreagated chunks:")
	if storeFlag {
		workerNode.IntermediateDataAggregated = AggregatedIntermediateTokens{
			ChunksSouces:                 mapJobs,
			PerReducerIntermediateTokens: perReducerIntermediateTokens,
		}
	}
	return perReducerIntermediateTokens
}
