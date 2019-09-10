package main

import (
	"../aws_SDK_wrap"
	"../core"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

/*

	//TODO FAULT TOLLERANT MAIN LINES
		-REDUCER FAULT=> reducer re instantiation (least loaded) mappers comunication of new reducer (re send intermediate data only to him)
		-WORKER MAPPER FAULT=> MAP jobs reassign considering chunks replication distribuition and REDUCER INTERMDIATE DATA AGGREGATED COSTRAINT	(SEE TODO IN REDUCER INT STATA)
							(e.g. multiple mapper worker fail after some REDUCE() => different expected REDUCE from reducers )
							WISE MAP JOBs REASSIGN AND SCHEDULE ....options:
								-todo not aggregated map result to route to reducers (avoid possibility intermd.data conflict on reducer)

TODO...
		@) al posto di heartbit monitoring continuo-> prendo errori da risultati di RPC e ne valuto successivamente soluzioni
			->eventuale heartbit check per vedere se worker è morto o solo istanza
*/

const INIT_FINAL_TOKEN_SIZE = 500
const TIMEOUT_PER_RPC time.Duration = time.Second * 2

func main() {
	core.Config = new(core.Configuration)
	core.Addresses = new(core.WorkerAddresses)
	core.ReadConfigFile(core.CONFIGFILENAME, core.Config)
	core.ReadConfigFile(core.ADDRESSES_GEN_FILENAME, core.Addresses)
	masterControl := core.MASTER_CONTROL{}
	var masterAddress string
	var err error
	var uploader *aws_SDK_wrap.UPLOADER = nil
	if core.Config.LOCAL_VERSION {
		masterAddress = "localhost"
		init_local_version(&masterControl)
	} else {

		////// master working config
		println("usage: isMasterCopy,publicIP master, data upload to S3, source file1,...source fileN")

		//// master address
		//masterAddress = "37.116.178.139" //dig +short myip.opendns.com @resolver1.opendns.com
		masterAddress = ""     //dig +short myip.opendns.com @resolver1.opendns.com
		if len(os.Args) >= 3 { //TODO SWTICH TO ARGV TEMPLATE
			masterAddress = os.Args[2]
		}
		//// master replica
		isMasterReplicaStr := "false"
		if len(os.Args) >= 2 { //TODO SWTICH TO ARGV TEMPLATE
			isMasterReplicaStr = os.Args[1]
		}
		if strings.Contains(strings.ToUpper(isMasterReplicaStr), "TRUE") {
			MasterReplicaStart(masterAddress)
		}
		//// load chunks flag
		loadChunksToS3 := false
		loadChunksToS3Str := "false"
		if len(os.Args) >= 4 {
			loadChunksToS3Str = os.Args[3]
		}
		if strings.Contains(strings.ToUpper(loadChunksToS3Str), "TRUE") {
			loadChunksToS3 = true
		}

		//// filenames
		filenames := core.FILENAMES_LOCL
		if len(os.Args) >= 5 { //TODO SWTICH TO ARGV TEMPLATE
			filenames = os.Args[4:]
		}
		masterControl.MasterAddress = masterAddress
		startInitTime := time.Now()
		uploader, _, err = core.Init_distribuited_version(&masterControl, filenames, loadChunksToS3)
		println("elapsed for initialization: ", time.Now().Sub(startInitTime).String())
		if core.CheckErr(err, false, "") {
			killAll(&masterControl.WorkersAll)
			os.Exit(96)
		}
	}
	masterData := masterRpcInit()
	masterControl.MasterRpc = masterData
	masterLogic(core.CHUNK_ASSIGN, &masterControl, uploader)
}

func masterLogic(startPoint uint32, masterControl *core.MASTER_CONTROL, uploader *aws_SDK_wrap.UPLOADER) {

	var startTime time.Time
	var err bool
	var errs []error
	//// from given starting point
	switch startPoint {
	case core.CHUNK_ASSIGN:
		goto chunk_assign
	case core.MAP_ASSIGN:
		goto map_assign
	case core.LOCALITY_AWARE_LINK_REDUCE:
		goto locality_aware_link_reduce
	}

	/////CHUNK ASSIGNEMENT
chunk_assign:
	if core.Config.BACKUP_MASTER {
		masterControl.State = core.CHUNK_ASSIGN
		masterControl.StateChan <- core.CHUNK_ASSIGN
		err := backUpMasterState(masterControl, uploader)
		if core.CheckErr(err, false, "") {
			_, _ = fmt.Fprint(os.Stderr, "MASTER STATE BACKUP FAILED, ABORTING")
			killAll(&masterControl.WorkersAll)
		}
	}

	/*
		fair distribuition of chunks among worker nodes with replication factor
			(will be assigned a fair share of chunks to each worker + a replication factor of chunks)
						(chunks replications R.R. of not already assigned chunks)
	*/
	startTime = time.Now()
	masterControl.MasterData.AssignedChunkWorkers = make(map[int][]int)
	masterControl.MasterData.AssignedChunkWorkersFairShare = assignChunksIDs(&masterControl.Workers.WorkersMapReduce, &(masterControl.MasterData.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR, false, masterControl.MasterData.AssignedChunkWorkers)
	assignChunksIDs(&masterControl.Workers.WorkersBackup, &(masterControl.MasterData.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS, true, masterControl.MasterData.AssignedChunkWorkers) //only replication assignement on backup workers

	errs = comunicateChunksAssignementToWorkers(masterControl.MasterData.AssignedChunkWorkers, &masterControl.Workers) //RPC 1 IN SEQ DIAGRAM
	println("elapsed for chunk assignement: ", time.Now().Sub(startTime).String())
	if len(errs) > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "ASSIGN CHUNK ERRD \n")
		workersIdsToReschedule := make(map[int][]int, len(errs)) //map of worker->chunks assigned to reschedule
		for _, err := range errs {
			workerIdErrd, _ := strconv.Atoi(err.Error()) //failed worker ID appended during comunication
			workerErrd := core.GetWorker(workerIdErrd, &(masterControl.Workers))
			workerErrd.State.Failed = true
			core.CheckErr(err, false, "failed worker id:"+strconv.Itoa(workerIdErrd))
			chunkIds := masterControl.MasterData.AssignedChunkWorkers[workerIdErrd]
			workersIdsToReschedule[workerIdErrd] = chunkIds
			//workerErrd,err:=core.GetWorker(workerIdErrd,&masterControl.Workers)
		}
		moreFailsIDs := core.PingProbeAlivenessFilter(masterControl) //filter in place failed workers
		//evaluate possible extra fails not reported in chunk assign errs reuturn (failed between assignEND<->ping filter
		for id, _ := range moreFailsIDs {
			chunksAssigned := masterControl.MasterData.AssignedChunkWorkersFairShare[id]
			if chunksAssigned != nil {
				workersIdsToReschedule[id] = chunksAssigned
			}
		}
		newChunksAssignements := AssignChunksIDsRecovery(&masterControl.Workers, workersIdsToReschedule,
			&(masterControl.MasterData.AssignedChunkWorkers), &(masterControl.MasterData.AssignedChunkWorkersFairShare))
		errs = comunicateChunksAssignementToWorkers(newChunksAssignements, &masterControl.Workers) //RPC 1 IN SEQ DIAGRAM
		if len(errs) > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "REASSIGNEMENT OF CHUNK ID FAILED...\n \t aborting all\n")
			killAll(&masterControl.WorkersAll)
			os.Exit(96)
		}

	}

	////	MAP
map_assign:
	if core.Config.BACKUP_MASTER {
		masterControl.State = core.MAP_ASSIGN
		<-masterControl.StateChan //TODO MASTER BACKUP OFF TEST
		masterControl.StateChan <- core.MAP_ASSIGN
		println("backup master state...")
		err := backUpMasterState(masterControl, uploader)
		if core.CheckErr(err, false, "") {
			_, _ = fmt.Fprint(os.Stderr, "MASTER STATE BACKUP FAILED, ABORTING")
			killAll(&masterControl.WorkersAll)
		}
	}

	/*
		assign individual map jobs to specific workers,
		they will retun control information about distribution of their intermediate token to (logic) reducers
		With these information logic reducers will be instantiated on workers exploiting intermediate data locality to route
	*/
	masterControl.MasterData.MapResults, err = assignMapWorks(masterControl.MasterData.AssignedChunkWorkersFairShare, &masterControl.Workers) //RPC 2,3 IN SEQ DIAGRAM
	if err {
		_, _ = fmt.Fprintf(os.Stderr, "ASSIGN MAP JOBS ERRD\n RETRY ON FAILED WORKERS EXPLOITING ASSIGNED CHUNKS REPLICATION")
		workerMapJobsToReassign := make(map[int][]int) //workerId--> map job to redo (chunkID previusly assigned)

		//// filter failed workers in map results
		filteredMapResults := make([]core.MapWorkerArgs, 0, len((*masterControl).MasterData.MapResults))
		moreFails := core.PingProbeAlivenessFilter(masterControl) //filter in place failed workers ( other eventually failed among calls
		for _, mapResult := range (*masterControl).MasterData.MapResults {
			failedWorker := core.CheckErr(mapResult.Err, false, "WORKER id:"+strconv.Itoa(mapResult.WorkerId)+" ON MAPS JOB ASSIGN") || moreFails[mapResult.WorkerId]
			if failedWorker {
				workerMapJobsToReassign[mapResult.WorkerId] = mapResult.ChunkIds
			} else {
				filteredMapResults = append(filteredMapResults, mapResult)
			}
		}
		((*masterControl).MasterData.MapResults) = filteredMapResults

		//re assign failed map job exploiting chunk replication among workers
		newMapBindings := AssignMapWorksRecovery(workerMapJobsToReassign, &masterControl.Workers, &(masterControl.MasterData.AssignedChunkWorkers))
		/// retry map
		mapResultsNew, err := assignMapWorks(newMapBindings, &masterControl.Workers)
		if err {
			_, _ = fmt.Fprintf(os.Stderr, "error on map RE assign\n aborting all")
			killAll(&masterControl.WorkersAll)
			os.Exit(96)
		}
		masterControl.MasterData.MapResults = mergeMapResults(mapResultsNew, masterControl.MasterData.MapResults)
	}

	////DATA LOCALITY AWARE REDUCER COMPUTATION && map intermadiate data set
locality_aware_link_reduce:
	if core.Config.BACKUP_MASTER {
		masterControl.State = core.LOCALITY_AWARE_LINK_REDUCE
		<-masterControl.StateChan //TODO MASTER BACKUP OFF TEST
		masterControl.StateChan <- core.LOCALITY_AWARE_LINK_REDUCE
		err := backUpMasterState(masterControl, uploader)
		if core.CheckErr(err, false, "") {
			_, _ = fmt.Fprint(os.Stderr, "MASTER STATE BACKUP FAILED, ABORTING")
			killAll(&masterControl.WorkersAll)
		}
	}
	masterControl.MasterData.ReducerRoutingInfos = aggregateMappersCosts(masterControl.MasterData.MapResults, &masterControl.Workers)
	masterControl.MasterData.ReducerSmartBindingsToWorkersID = core.ReducersBindingsLocallityAwareEuristic(masterControl.MasterData.ReducerRoutingInfos.DataRoutingCosts, &masterControl.Workers)
	////DATA LOCALITY AWARE REDUCER BINDINGS COMMUNICATION
	/*
		instantiate logic reducer on actual worker and communicate  these bindings to workers with map instances
		they will aggregate reduce calls to individual reducers propagating  eventual errors
	*/

	rpcErrs := comunicateReducersBindings(masterControl) //RPC 4,5 IN SEQ DIAGRAM;
	if len(errs) > 0 {
		//set up list for failed instances inside failed workers (mapper & reducer)
		core.PingProbeAlivenessFilter(masterControl) //filter in place failed workers
		mapToRedo, reduceToRedo := core.ParseReduceErrString(rpcErrs, masterControl.MasterData.ReducerSmartBindingsToWorkersID, &masterControl.Workers)
		///MAPS REDO
		var newMapBindings map[int][]int = nil
		if len(mapToRedo) > 0 {
			newMapBindings = AssignMapWorksRecovery(mapToRedo, &masterControl.Workers, &(masterControl.MasterData.AssignedChunkWorkers)) //re bind maps work to workers
			mapResultsNew, err := assignMapWorks(newMapBindings, &masterControl.Workers)                                                 //re do maps
			_ = aggregateMappersCosts(mapResultsNew, &masterControl.Workers)                                                             //TODO USELESS IF NOT MULTIPLE RETRIED THIS PHASE
			core.GenericPrint(mapResultsNew, "")
			//mapResults=mergeMapResults(mapResultsNew,mapResults)
			if err {
				_, _ = fmt.Fprintf(os.Stderr, "error on map RE assign\n aborting ...")
				killAll(&masterControl.WorkersAll)
				os.Exit(96)
			}
		}

		///REDUCE RESTART on failed reducers
		if len(reduceToRedo) > 0 {
			//re assign failed reducer on avaible workers following custom order for better assignements in accord with load balance
			newReducersBindings := ReducersReplacementRecovery(reduceToRedo, newMapBindings, &masterControl.MasterData.ReducerSmartBindingsToWorkersID, &masterControl.Workers)
			core.GenericPrint(newReducersBindings, "")
		}
		//as before comunicate newly re spawned reducer on worker -> NB mappers will trasparently re send per reducer intermediate data
		errs := comunicateReducersBindings(masterControl)
		if errs != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error on map RE reduce comunication")
			killAll(&masterControl.WorkersAll)
			os.Exit(96)
		}
	}
	// will be triggered in 6
	jobsEnd(masterControl) //wait all reduces END then, kill all workers
	if core.Config.SORT_FINAL {
		tk := core.TokenSorter{masterControl.MasterRpc.FinalTokens}
		sort.Sort(sort.Reverse(tk))
	}
	core.SerializeToFile(masterControl.MasterRpc.FinalTokens, core.FINAL_TOKEN_FILENAME)
	println(masterControl.MasterRpc.FinalTokens)
	os.Exit(0)
}

func init_local_version(control *core.MASTER_CONTROL) {
	////// init files
	var filenames []string = core.FILENAMES_LOCL
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}

	////// load chunk to storage service
	control.MasterData.ChunkIDS = core.LoadChunksStorageService_localMock(filenames)
	////// init workers
	control.Workers, control.WorkersAll = core.InitWorkers_LocalMock_MasterSide() //TODO BOTO3 SCRIPT CONCURRENT STARTUP
	//creating workers ref
}

func masterRpcInit() *core.MasterRpc {
	//register master RPC
	reducerEndChan := make(chan bool, core.Config.ISTANCES_NUM_REDUCE) //buffer all reducers return flags for later check (avoid block during rpc return )
	master := core.MasterRpc{
		FinalTokens:     make([]core.Token, 0, INIT_FINAL_TOKEN_SIZE),
		Mutex:           core.MUTEX{},
		ReturnedReducer: &reducerEndChan,
	}
	server := rpc.NewServer()
	err := server.RegisterName("MASTER", &master)
	core.CheckErr(err, true, "master rpc register errorr")
	l, e := net.Listen("tcp", ":"+strconv.Itoa(core.Config.MASTER_BASE_PORT))
	core.CheckErr(e, true, "socket listen error")
	go server.Accept(l)
	return &master
}
func assignChunksIDs(workers *[]core.Worker, chunksIds *[]int, replicationFactor int, onlyReplication bool, globalChunkAssignement map[int][]int) map[int][]int {
	/*
		fair share of chunks assigned to each worker plus a replication factor of chunks
		only latter if onlyReplication is true
		global assignement  handled by a global var globalChunkAssignement for replication
		fairShare of chunks needed for map assigned to a special field of worker
		return the fair share assignements to each worker passed,

	*/
	if len(*workers) == 0 {
		return nil
	}
	_fairChunkNumShare := len(*chunksIds) / len(*workers)
	fairChunkNumShare := int(len(*chunksIds) / len(*workers))
	if _fairChunkNumShare < 1 {
		fairChunkNumShare = 1
	}
	assignementFairShare := make(map[int][]int, len(*workers))
	chunkIndex := 0
	if !onlyReplication { //evaluate both fair assignement and replication
		for i, worker := range *workers {
			if chunkIndex >= len(*chunksIds) {
				break //too few chunks for workers ammount
			}
			chunkIDsFairShare := (*chunksIds)[i*fairChunkNumShare : (i+1)*fairChunkNumShare]
			globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunkIDsFairShare...) //quicklink for smart replication
			assignementFairShare[worker.Id] = chunkIDsFairShare
			workerChunks := &(worker.State.ChunksIDs)
			*workerChunks = append((*workerChunks), chunkIDsFairShare...)
			chunkIndex += fairChunkNumShare

		}

		if (len(*chunksIds) - chunkIndex) > 0 { //chunks residues not assigned yet
			//println("chunk fair share remainder (-eq)", len(*chunksIds)-chunkIndex, fairChunkNumRemider)
			//last worker will be in charge for last chunks
			worker := (*workers)[len(*workers)-1]
			lastShare := (*chunksIds)[chunkIndex:]
			globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], lastShare...)
			assignementFairShare[worker.Id] = append(assignementFairShare[worker.Id], lastShare...)

		}
	}
	//CHUNKS REPLICATION
	for _, worker := range *workers {
		////reminder assignment //todo old
		//worker.State.ChunksIDs = append(worker.State.ChunksIDs, chunkIDsFairShareReminder...)                   // will append an empty list if OnlyReplciation is true//TODO CHECK DEBUG
		//globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunkIDsFairShareReminder...) //quick link for smart replication
		////replication assignment
		chunksReplicationToAssignID, err := core.GetChunksNotAlreadyAssignedRR(chunksIds, replicationFactor, globalChunkAssignement[worker.Id])
		core.CheckErr(err, false, "chunks replication assignment impossibility, chunks saturation on workers")
		globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunksReplicationToAssignID...) //quick link for smart replication
	}
	return assignementFairShare
}

func comunicateChunksAssignementToWorkers(assignementChunkWorkers map[int][]int, workers *core.WorkersKinds) []error {
	/*
		comunicate to all workers chunks assignements
		propagate eventual errors containing stringified failed workerID
	*/
	ends := make([]*rpc.Call, len(assignementChunkWorkers))
	i := 0
	errs := make([]error, 0)
	//ii := 0
	workersAssigned := make([]*core.Worker, len(assignementChunkWorkers))
	for workerId, chunksIds := range assignementChunkWorkers {
		if len(chunksIds) > 0 {
			workerPntr := core.GetWorker(workerId, workers)
			time.Sleep(time.Second * 2)
			ends[i] = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Go("CONTROL.Get_chunk_ids", chunksIds, nil, nil)
			workersAssigned[i] = workerPntr
			i++
		}
	}
	var divCall *rpc.Call
	//timeout:=TIMEOUT_PER_RPC
	//startTime:=time.Now()
	for i, doneChan := range ends { //wait all assignment compleated
		hasTimedOut := false
		if doneChan != nil {
			select {
			case divCall = <-doneChan.Done:
				//case <-time.After(TIMEOUT_PER_RPC):
				//	{
				//		_, _ = fmt.Fprintf(os.Stderr, "RPC TIMEOUT\n")
				//		hasTimedOut = true
				//	}
			}
			worker := *(workersAssigned[i])
			if hasTimedOut || core.CheckErr(divCall.Error, false, "chunkAssign Res on"+strconv.Itoa(worker.Id)) {
				//errors=append(errors,divCall.Error)
				errs = append(errs, errors.New(strconv.Itoa(worker.Id))) //append worker id of failed rpc
				continue
			}
			worker.State.ChunksIDs = append(worker.State.ChunksIDs, assignementChunkWorkers[worker.Id]...) //eventually append correctly assigned chunk to worker
		}
	}
	return errs
}

func assignMapWorks(workerMapperChunks map[int][]int, workers *core.WorkersKinds) ([]core.MapWorkerArgs, bool) {
	/*
		assign MAP input data (chunks) to designed workers that will trigger several concurrent MAP execution
		intermediate tokens data buffered inside workers and routing cost of data to reducers (logic) returned aggregated at worker node level
		that reflect data locality of interm.data over workers,
	*/

	hasErrd := false
	mapRpcWrap := make([]core.MapWorkerArgs, len(workerMapperChunks))
	i := 0
	for workerId, chunkIDs := range workerMapperChunks {
		workerPntr := core.GetWorker(workerId, workers)
		mapRpcWrap[i] = core.MapWorkerArgs{
			ChunkIds: chunkIDs,
			WorkerId: workerId,
			Err:      nil,
			Reply:    core.Map2ReduceRouteCost{},
		}
		//async start map
		mapRpcWrap[i].End = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Go("CONTROL.DoMAPs", mapRpcWrap[i].ChunkIds, &(mapRpcWrap[i].Reply), nil)
		i++
	}
	for _, mapArg := range mapRpcWrap {
		<-mapArg.End.Done
		err := mapArg.End.Error
		if core.CheckErr(err, false, "error on workerMap:"+strconv.Itoa(mapArg.WorkerId)) {
			mapArg.Err = err
			hasErrd = true
		}
	}
	return mapRpcWrap, hasErrd
}

func aggregateMappersCosts(workerMapResults []core.MapWorkerArgs, workers *core.WorkersKinds) core.ReducersRouteInfos {
	//for each mapper worker aggregate route infos

	//nested dict for route infos
	workersMapRouteCosts := make(map[int]map[int]int, len(workerMapResults))
	workersMapExpectedReduceCalls := make(map[int]map[int]int, len(workerMapResults))
	for _, workerResult := range workerMapResults {
		//init aggreagate infos nested dicts
		workerId := workerResult.WorkerId
		worker := core.GetWorker(workerId, workers)
		worker.State.MapIntermediateTokenIDs = append(worker.State.MapIntermediateTokenIDs, workerResult.ChunkIds...) //set intermadiate data inside worker
		workersMapRouteCosts[workerId] = make(map[int]int, core.Config.ISTANCES_NUM_REDUCE)
		workersMapExpectedReduceCalls[workerId] = make(map[int]int, core.Config.ISTANCES_NUM_REDUCE)
		//aggreagate infos
		for reducer, routeCostTo := range workerResult.Reply.RouteCosts {
			workersMapRouteCosts[workerId][reducer] = routeCostTo
		}
		for reducer, expectedCallsToFromMapper := range workerResult.Reply.RouteNum {
			workersMapExpectedReduceCalls[workerId][reducer] = expectedCallsToFromMapper
		}
	}
	routeInfosAggregated := core.ReducersRouteInfos{
		DataRoutingCosts:           core.ReducersDataRouteCosts{workersMapRouteCosts},
		ExpectedReduceCallsMappers: workersMapExpectedReduceCalls,
	}
	return routeInfosAggregated
}

func comunicateReducersBindings(control *core.MASTER_CONTROL) []error {
	//for each reducer ID (logic) activate an actual reducer on a worker following redBindings dict
	// init each reducer with expected reduce calls from mappers indified by their assigned chunk (that has produced map result -> reduce calls)
	//RPC 4,5 in SEQ diagram

	redNewInstancePort := 0
	errs := make([]error, 0)

	/// RPC 4 ---> REDUCERS INSTANCES ACTIVATION
	reducerBindings := make(map[int]string, core.Config.ISTANCES_NUM_REDUCE)
	for reducerIdLogic, placementWorker := range control.MasterData.ReducerSmartBindingsToWorkersID {
		println("reducer with logic ID: ", reducerIdLogic, " on worker : ", placementWorker)
		worker := core.GetWorker(placementWorker, &control.Workers) //get dest worker for the new Reduce Instance
		//instantiate the new reducer instance with expected calls # from mapper for termination and faultTollerant
		arg := core.ReduceActiveArg{
			NumChunks: len(control.MasterData.ChunkIDS),
			LogicID:   reducerIdLogic,
		}
		err := (*rpc.Client)(worker.State.ControlRPCInstance.Client).Call("CONTROL.ActivateNewReducer", arg, &redNewInstancePort)
		if core.CheckErr(err, false, "instantiating reducer: "+strconv.Itoa(reducerIdLogic)) {
			errs = append(errs, errors.New(core.REDUCER_ACTIVATE+core.ERROR_SEPARATOR+strconv.Itoa(reducerIdLogic)))
		}
		reducerBindings[reducerIdLogic] = worker.Address + ":" + strconv.Itoa(redNewInstancePort) //note the binding to reducer correctly instantiated
		worker.State.ReducersHostedIDs = append(worker.State.ReducersHostedIDs, reducerIdLogic)
	}
	if len(errs) > 0 {
		_, _ = fmt.Fprint(os.Stderr, "some reducers activation has failed")
		return errs
	}
	/// RPC 5 ---> REDUCERS BINDINGS COMUNICATION TO MAPPERS WORKERS
	//comunicate to all mappers final Reducer location
	ends := make([]*rpc.Call, 0, len(control.MasterData.MapResults))
	mappersErrs := make([][]error, len(control.MasterData.MapResults))
	//endsRpc:=make([]*rpc.Call,0,len(workers.WorkersMapReduce)*len(workers.WorkersMapReduce[0].State.WorkerIstances))
	for i, workerMapRes := range control.MasterData.MapResults {
		worker := core.GetWorker(workerMapRes.WorkerId, &(control.Workers))
		callAsync := (*rpc.Client)(worker.State.ControlRPCInstance.Client).Go("CONTROL.ReducersCollocations", reducerBindings, &(mappersErrs[i]), nil)
		ends = append(ends, callAsync)
	}
	//wait rpc return
	for i, end := range ends {
		<-end.Done
		if core.CheckErr(end.Error, false, "error on bindings comunication") {
			errs = append(errs, errors.New(core.REDUCERS_ADDR_COMUNICATION+core.ERROR_SEPARATOR+strconv.Itoa(control.Workers.WorkersMapReduce[i].Id)))
		}
	}
	return errs
}

func killAll(workers *[]core.Worker) {
	//now reducers has returned, workers can end safely
	for _, worker := range *workers {
		println("Exiting worker: ", worker.Id)
		err := (*rpc.Client)(worker.State.ControlRPCInstance.Client).Call("CONTROL.ExitWorker", 0, nil)
		core.CheckErr(err, false, "error in shutdowning worker: "+strconv.Itoa(worker.Id))
	}
}
func jobsEnd(control *core.MASTER_CONTROL) {
	/*
		block main thread until REDUCERS workers will comunicate that all REDUCE jobs are ended
	*/
	for r := 0; r < core.Config.ISTANCES_NUM_REDUCE; r++ {
		<-*(*control).MasterRpc.ReturnedReducer //wait end of all reducers
		println("ENDED REDUCER!!!", r)
	}
	killAll(&(control.WorkersAll))
}
