package main

import (
	"../aws_SDK_wrap"
	"../core"
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const LOCAL_MASTER_REPLICA = true

func ReducersReplacementRecovery(failedWorkersReducers map[int][]int, oldReducersBindings map[int]int, workersKinds *core.WorkersKinds) map[int]int {
	//re decide reduce placement on workers selecting a random healty worker
	//returned new bindings and updated in place old

	failedReducersIDs := make([]int, 0, len(failedWorkersReducers))
	newReducersPlacements := make(map[int]int) //new placement for failed reducers
	// setup workers in preferential order of reducer reschedule
	workers := append(workersKinds.WorkersOnlyReduce, workersKinds.WorkersBackup...)
	workers = append(workers, workersKinds.WorkersMapReduce...)
	//filter active workers and bindings
	for _, worker := range workers {
		reducersIDs, failedWorker := failedWorkersReducers[worker.Id]
		failedWorker = worker.State.Failed || failedWorker
		if failedWorker {
			worker.State.Failed = true
			for i := 0; i < len(reducersIDs); i++ {
				delete((oldReducersBindings), reducersIDs[i]) //remove stale assignement
			}
		}
	}
	for _, reducerIDs := range failedWorkersReducers {
		failedReducersIDs = append(failedReducersIDs, reducerIDs...)
	}
	core.GenericPrint(failedReducersIDs, "failed reducersIDs: ")
	/// re assign  reducers to workers
	reducersPerWoker := 1
	reducersPerWokerResidue := 0
	if len(failedReducersIDs) > len(workers) {
		reducersPerWoker = len(failedReducersIDs) / len(workers)
		reducersPerWokerResidue = len(failedReducersIDs) % len(workers)
	}
	reducerToAssign := 0
	for i, worker := range workers {
		reducersForWorker := reducersPerWoker
		if i == len(workers)-1 {
			reducersForWorker += reducersPerWokerResidue
		}
		for i := 0; i < reducersForWorker; i++ {
			failedReducerID := failedReducersIDs[reducerToAssign+i]
			///FINALLY RE PLACE FAILED REDUCER
			newReducersPlacements[failedReducerID] = worker.Id //newly replaced reducer
			(oldReducersBindings)[failedReducerID] = worker.Id
		}
		reducerToAssign += reducersPerWoker
		if reducerToAssign >= len(failedReducersIDs) {
			break
		}
	}
	return newReducersPlacements
}

func MapPhaseRecovery(masterControl *core.MASTER_CONTROL, failedJobs map[int][]int, moreFails map[int]bool, uploader *aws_SDK_wrap.UPLOADER) bool {
	println("MAP RECOVERY")
	//// filter failed workers in map results
	workerMapJobsToReassign := make(map[int][]int) //workerId--> map job to redo (chunkID previusly assigned)

	///// evalutate to terminate
	if len(masterControl.WorkersAll) < core.Config.MIN_WORKERS_NUM {
		_, _ = fmt.Fprint(os.Stderr, "TOO MUCH WORKER FAILS... ABORTING COMPUTATION..")
		killAll(&masterControl.Workers)
		os.Exit(96)
	}
	//get list of lost foundamental map jobs because of worker fails
	for workerID, _ := range masterControl.MasterData.AssignedChunkWorkersFairShare {
		_, failedWorker := moreFails[workerID]
		if failedWorker {
			workerMapJobsToReassign[workerID] = masterControl.MasterData.AssignedChunkWorkersFairShare[workerID]
			println("worker maps lost :", workerID, " # -> ", len(workerMapJobsToReassign[workerID]))
		}
	}

	if failedJobs != nil { //eventually insert passed failed jobs
		for workerID, jobs := range failedJobs {
			_, alreadyKnowWorkerFailed := workerMapJobsToReassign[workerID]
			if !alreadyKnowWorkerFailed {
				workerMapJobsToReassign[workerID] = jobs
			}
		}
	}
	//workerMapJobsToReassign ,_=core.GetLostJobsGeneric(&masterControl.MasterData,moreFails)

	checkMapRes(masterControl)
	newChunks := AssignChunksIDsRecovery(&masterControl.Workers, workerMapJobsToReassign, (masterControl.MasterData.AssignedChunkWorkers), (masterControl.MasterData.AssignedChunkWorkersFairShare))
	//re assign failed map job exploiting chunk replication among workers
	if len(workerMapJobsToReassign) == 0 || len(newChunks) == 0 {
		println("nothing to recovery")
		return true //nothing todo --> backup worker failed or chunk replication already solved the problem
	}

	//// checkpoint master state updating  chunk fair share assignments
	if core.Config.BACKUP_MASTER {
		backUpMasterState(masterControl, uploader)
	}
	/// retry map
	((*masterControl).MasterData.MapResults) = make([]core.MapWorkerArgsWrap, 0, len((*masterControl).MasterData.MapResults))
	mapResultsNew, err := assignMapJobsWithRedundancy(&masterControl.MasterData, &masterControl.Workers, newChunks, masterControl.MasterData.AssignedChunkWorkersFairShare) //RPC 2,3 IN SEQ DIAGRAM
	//masterControl.MasterData.MapResults = mapResultsNew
	masterControl.MasterData.MapResults = mergeMapResults(masterControl.MasterData.MapResults, mapResultsNew)
	if err {
		_, _ = fmt.Fprintf(os.Stderr, "error on map RE assign")
		print(&mapResultsNew)
		return false
	}
	//TODO DEBUG CHECKS
	checkMapRes(masterControl)
	if len(masterControl.MasterData.MapResults) == 0 {
		killAll(&masterControl.Workers)
		panic("NO MAP RESULT")
	}

	return true
}
func AssignChunksIDsRecovery(workerKinds *core.WorkersKinds, workerChunksFailed map[int][]int, oldAssignementsGlbl, oldAssignementsFairShare map[int][]int) map[int][]int {
	checkMapRes(&MasterControl)
	//reassign chunks evaluating existent replication old assignmenets and reducing assignement only at fundamental chunk to reassign
	//old assignements map will be modified in place

	failedChunk := make(map[int]bool, len(oldAssignementsGlbl))
	workers := append(workerKinds.WorkersBackup, workerKinds.WorkersMapReduce...) //preferential order for chunk re assignement
	workers = append(workers, workerKinds.WorkersOnlyReduce...)                   //preferential order for chunk re assignement
	//extract chunks to reassign filtering duplications introduced for fault tollerant (less reassignment here)
	for workerId, _ := range workerChunksFailed {
		delete(oldAssignementsGlbl, workerId)      //DELETE FAILED WORKER KEY FROM ASSIGNEMENT DICT
		delete(oldAssignementsFairShare, workerId) //DELETE FAILED WORKER KEY FROM ASSIGNEMENT DICT
	}

	reverseGlblChunkMap := make(map[int]int)
	for workerID, chunks := range oldAssignementsGlbl {
		for _, chunkID := range chunks {
			reverseGlblChunkMap[chunkID] = workerID
		}
	}
	reverseChunkMapFairShare := make(map[int]int)
	backup := make(map[int][]int)
	for workerID, chunks := range oldAssignementsFairShare {
		backup[workerID] = chunks
		core.GenericPrint(chunks, "worker:"+strconv.Itoa(workerID))
		for _, chunkID := range chunks {
			reverseChunkMapFairShare[chunkID] = workerID
		}
	}
	///// select foundamental chunk to reassign in accord with prev replication, eventually append to fair share missing chunk
	for _, chunks := range workerChunksFailed {
		for _, chunkId := range chunks {
			workerInPossess, isAlreadyAssigned := reverseGlblChunkMap[chunkId]
			_, presentInFairShare := reverseChunkMapFairShare[chunkId] //todo redundant
			if !isAlreadyAssigned && !presentInFairShare {
				failedChunk[chunkId] = true
			} else if !presentInFairShare { //update fair share with prev. redundant chunk now useful
				oldAssignementsFairShare[workerInPossess] = append(oldAssignementsFairShare[workerInPossess], chunkId)

				reverseChunkMapFairShare[chunkId] = workerInPossess
				println("exploited data locality of chunk: ", chunkId, "on worker ", workerInPossess)
			}
		}
	}
	//// only not already assigned chunk will be scheduled for re download on other active workers
	unrecoverableChunks := make([]int, 0, len(workerChunksFailed))
	for chunkID, _ := range failedChunk {
		unrecoverableChunks = append(unrecoverableChunks, chunkID)
	}
	core.GenericPrint(unrecoverableChunks, "CHUNKS TO REASSIGN")

	//select avaible worker not failed and set new assignement until all foundamental chunk are assigned or not enought workers
	chunksToReassignPerWorker := 1
	chunksToReassignPerWorkerResidiue := 0
	if len(unrecoverableChunks) > len(workers) {
		chunksToReassignPerWorker = int(len(unrecoverableChunks) / len(workers))
		chunksToReassignPerWorkerResidiue = len(unrecoverableChunks) % len(workers)
	}
	chunkToAssignIndexEnd := 0
	assignedChunks := 0
	newAssignements := make(map[int][]int)
	checkMapRes(&MasterControl)
	for i := 0; i < len(workers) && assignedChunks < len(unrecoverableChunks); i++ {
		chunkToAssignIndexEnd = assignedChunks + chunksToReassignPerWorker
		if i == len(workers)-1 {
			chunkToAssignIndexEnd += chunksToReassignPerWorkerResidiue //for last worker eventually assign residue jobs
		}
		chunksPerWorker := unrecoverableChunks[assignedChunks:chunkToAssignIndexEnd]
		workerId := (workers)[i].Id
		//// assignement recovery
		core.GenericPrint(chunksPerWorker, "assigning to : "+strconv.Itoa(workerId))
		newAssignements[workerId] = chunksPerWorker                                                             //only new assignements map set
		(oldAssignementsGlbl)[workerId] = append((oldAssignementsGlbl)[workerId], chunksPerWorker...)           //update in place assignements
		(oldAssignementsFairShare)[workerId] = append((oldAssignementsFairShare)[workerId], chunksPerWorker...) //update in place assignements

		assignedChunks += chunksToReassignPerWorker
	} //compleated assignements
	checkMapRes(&MasterControl)
	return newAssignements
}

func mergeMapResults(mapResBase []core.MapWorkerArgsWrap, mapRes2 []core.MapWorkerArgsWrap) []core.MapWorkerArgsWrap {
	//merge a base list of map results filtering failed maps job and appending mapRes2 to it
	mapResultsMerged := make([]core.MapWorkerArgsWrap, 0, len(mapResBase))
	for _, mapperRes := range mapResBase {
		if len(mapperRes.Err) == 0 { //good result from old map output
			mapResultsMerged = append(mapResultsMerged, mapperRes)
		}
	}
	return append(mapResultsMerged, mapRes2...)
}

func MasterReplicaStart(myAddr string) {
	//master replica logic, ping probe real master, on fault read from stable storage old master state and recovery from there...
	//master replica has to be in the same subnet  of the running master or  tunelled to the same public address published before
	//because the re-publish of the master addr need extra logic on worker side for retrieving it

	masterDead := false
	downloader, uploader := aws_SDK_wrap.InitS3Links(core.Config.S3_REGION)

	masterAddr, err := core.GetMasterAddr(downloader, core.Config.S3_BUCKET, core.MASTER_ADDRESS_PUBLISH_S3_KEY)
	masterAddrIP := (strings.Split(masterAddr, ":"))[0]
	if LOCAL_MASTER_REPLICA {
		masterAddrIP = "127.0.0.1"
	}
	masterAddr = masterAddrIP + ":" + strconv.Itoa(core.Config.PING_SERVICE_BASE_PORT)

	//heart bit monitoring running master -> on fault take control
	var masterState byte = 0
	pollingPing := time.Millisecond * (time.Duration(core.Config.PING_TIMEOUT_MILLISECONDS))
	for {
		err, masterState = core.PingHeartBitSnd(masterAddr)
		if err != nil {
			masterDead = true
			break
		}
		println("received state from running master:", masterState)
		if masterState == core.ENDED {
			break
		}
		time.Sleep(pollingPing)
	}
	if !masterDead { //if bool not setted during running master heartbit monitoring all program instances can exit
		println("MASTER CORRECTLY ENDED\tREPLICA SHUTDOWN...")
		os.Exit(0)
	}
	println("RUNNING MASTER SEEM TO BE DOWN...\tLAST SEEN STATE:\t", masterState)
	//recover master state from stable storage on S3 deserializing base64 -> gob decoding -> master control struct
	masterControl, err := RecoveryFailedMasterState(downloader)
	core.CheckErr(err, true, "FAILED TO RECOVERY  MASTER STATE...aborting")

	//update replica specific infos in master struct
	masterControl.MasterAddress = myAddr
	chunks := core.InitChunks(core.FILENAMES_LOCL) //chunkize filenames
	masterControl.MasterData.Chunks = chunks       //save in memory loaded chunks -> they will not be backup in master checkpointing
	masterControl.StateChan = make(chan uint32, 5*core.Config.FAIL_RETRY)
	masterControl.MasterRpc = masterRpcInit()
	masterControl.IsReplica = true

	fails := refreshConnections(&masterControl)
	masterControl.FailsAtStart = len(fails) > 0
	//// restart master logic from loaded old master state
	masterLogic(masterControl.State, &masterControl, true, uploader)
}

const MASTER_STATE_S3_KEY = "MASTER_STATE_DUMP"
const MASTER_STATE_SIZE_EXCESS = 50000000

func backUpMasterState(control *core.MASTER_CONTROL, uploader *aws_SDK_wrap.UPLOADER) {
	//backup master state to stable storage

	var err error = nil
	base64OfState := core.SerializeMasterStateBase64(*control)
	err = aws_SDK_wrap.UploadDATA(uploader, base64OfState, MASTER_STATE_S3_KEY, core.Config.S3_BUCKET)
	println("serialized master state in stable, fault tollerant  storage")

	if core.CheckErr(err, false, "MASTER STATE BACKUP FAILED, ABORTING") {
		killAll(&control.Workers)
		os.Exit(1)
	}
}
func RecoveryFailedMasterState(downloader *aws_SDK_wrap.DOWNLOADER) (core.MASTER_CONTROL, error) {
	//recovery master state previusly uploaded to stable storage (S3)
	buf := make([]byte, MASTER_STATE_SIZE_EXCESS)
	err := aws_SDK_wrap.DownloadDATA(downloader, core.Config.S3_BUCKET, MASTER_STATE_S3_KEY, &buf, false)
	if core.CheckErr(err, false, "master old state recovery err") {
		return core.MASTER_CONTROL{}, err
	}
	out := core.DeSerializeMasterStateBase64(string(buf))
	return out, nil
}

func refreshConnections(masterControl *core.MASTER_CONTROL) map[int]bool {
	//// estamblish new connection to workers recovered evaluating exiting on too much fails
	// when all connection has been re estamblished will be waited that worker are in IDLE state to avoid race condtion on rpc methods
	//return eventual more fails from ping alivness filter
	workersKindsNums := map[string]int{
		core.WORKERS_MAP_REDUCE: len(masterControl.Workers.WorkersMapReduce), core.WORKERS_ONLY_REDUCE: len(masterControl.Workers.WorkersOnlyReduce),
		core.WORKERS_BACKUP_W: len(masterControl.Workers.WorkersBackup),
	}
	var sourceWorkersContainer *[]core.Worker //dest variable for workers to init
	failedWorkers := make(map[int]bool)
	for workerKind, numToREInit := range workersKindsNums {
		println("initiating: ", numToREInit, "of workers kind: ", workerKind)
		//taking worker kind addresses list
		if workerKind == core.WORKERS_MAP_REDUCE {
			sourceWorkersContainer = &(masterControl.Workers.WorkersMapReduce)
		} else if workerKind == core.WORKERS_ONLY_REDUCE {
			sourceWorkersContainer = &(masterControl.Workers.WorkersOnlyReduce)
		} else if workerKind == core.WORKERS_BACKUP_W {
			sourceWorkersContainer = &(masterControl.Workers.WorkersBackup)
		}
		for i := 0; i < numToREInit; i++ {
			// inplace restore worker rpc client
			worker := &((*sourceWorkersContainer)[i])
			workerRpcAddr := worker.Address + ":" + strconv.Itoa(worker.State.ControlRPCInstance.Port)

			rpcClientChan := make(chan *rpc.Client, 1)
			var err error
			var cli *rpc.Client
			go func() {
				cli, err = rpc.Dial(core.Config.RPC_TYPE, workerRpcAddr)
				if !core.CheckErr(err, false, "Dial worker at "+workerRpcAddr) {
					rpcClientChan <- cli
				}
			}()
			var client *rpc.Client
			select {
			case client = <-rpcClientChan:
			case <-time.After(time.Duration(core.Config.WORKER_DIAL_TIMEOUT) * time.Second):
				err = errors.New("DIAL TIMEDOUT FOR WORKER AT " + workerRpcAddr)
			}

			if core.CheckErr(err, false, "worker conn refresh failed") {
				worker.State.Failed = true
				failedWorkers[worker.Id] = true
				continue
			}
			println("refreshed connection to worker:\t", workerRpcAddr)
			worker.State.ControlRPCInstance.Client = (*core.CLIENT)(client)
		}
	}

	morefails := core.PingProbeAlivenessFilter(masterControl, true)
	println("while refreshing workers connection founded failed worker: ", len(failedWorkers), "\t more fails:", len(morefails))
	///// wait until all worker are idle and re filter away eventual failed worker in between
	/// evaluate to exit on too much faults
	if len(masterControl.WorkersAll)-int(core.Max(int64(len(morefails)), int64(len(failedWorkers)))) < core.Config.MIN_WORKERS_NUM {
		_, _ = fmt.Fprint(os.Stderr, "TOO MUTCH FAILS...")
		killAll(&masterControl.Workers)
		os.Exit(96)
	}
	return morefails
}

/*
	master replica on master fault will recovery eventual lost worker result calling these 2 function
	that have the same return values of original triggers for MAP() & REDUCE() in non faulty master logic
	worker will cache answers for master so he will answer to eventual master replica spawn
*/

func RecoveryMapResults(control *core.MASTER_CONTROL) ([]core.MapWorkerArgsWrap, bool) {
	//retrieve map result from workers following fair chunk assignment and  set eventual errors
	//return retrieved result and true if something has returned fails
	mapResultReFetched := make([]core.MapWorkerArgsWrap, 0, len(control.Workers.WorkersMapReduce))
	hasFailed := false
	var err error
	var reply core.Map2ReduceRouteCost
	for workerID, chunks := range control.MasterData.AssignedChunkWorkersFairShare {
		workerPntr := core.GetWorker(workerID, &control.Workers, false)
		core.GenericPrint(chunks, "recovering map result from worker:\t"+strconv.Itoa(workerID))
		if workerPntr != nil && !workerPntr.State.Failed {
			//TRY RECOVER MAP RESULT WITH TIMEOUT ON THE RECOVERY RPC
			err = nil
			go func() {
				err = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Call("CONTROL.RecoveryMapRes", chunks, &reply)
				syncRpcTimeoutChan <- err
				runtime.Goexit()
			}()
			select {
			case err = <-syncRpcTimeoutChan:
			case <-time.After(core.TIMEOUT_PER_RPC):
				err = errors.New(core.ERR_TIMEOUT_RPC)
			}
			if core.CheckErr(err, false, "MAP RECOVERY FROM REPLICA ERR AT WORKER"+strconv.Itoa(workerID)) {
				err = errors.New("failed worker ")
				hasFailed = true
			}
		} else {
			err = errors.New(core.ERR_TIMEOUT_RPC)
			hasFailed = true
		}
		mapResultReFetched = append(mapResultReFetched,
			core.MapWorkerArgsWrap{
				WorkerId: workerID,
				Reply:    reply,
				MapJobArgs: core.MapWorkerArgs{
					ChunkIds: chunks,
				},
			})
		if err != nil {
			mapResultReFetched[len(mapResultReFetched)-1].Err = err.Error() //set error field with corresponding string if exist
		}
	}
	println("Recovery map result global result: ", hasFailed)
	return mapResultReFetched, hasFailed
}

func RecoveryReduceResults(control *core.MASTER_CONTROL) bool {
	//try to recover reducers results, if some fails has happened reset them and later retry saving map computations
	fails := false
	var err error = nil
	reducersResults := make([]core.ReduceRet, core.Config.ISTANCES_NUM_REDUCE)
	for redLogicID, workerID := range control.MasterData.ReducerSmartBindingsToWorkersID {
		workerPntr := core.GetWorker(workerID, &control.Workers, false)
		println("recovering final token share from reducer id :", redLogicID, "at :", workerPntr.Address)
		if workerPntr == nil || workerPntr.State.Failed {
			fails = true
			continue
		}
		go func() {
			err = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Call("CONTROL.RecoveryReduceResult", redLogicID, &reducersResults[redLogicID])
			syncRpcTimeoutChan <- err
			runtime.Goexit()
		}()
		select {
		case err = <-syncRpcTimeoutChan:
		case <-time.After(core.TIMEOUT_PER_RPC):
			err = errors.New(core.ERR_TIMEOUT_RPC)
		}
		if core.CheckErr(err, false, "reduce recovery err") {
			fails = true
			continue
		}
		_ = control.MasterRpc.ReturnReduce(reducersResults[redLogicID], nil)
	}
	/*if !fails {
		//////////// aggregate if all result has been retrieved
		for _, redResult := range reducersResults {
			_ = control.MasterRpc.ReturnReduce(redResult, nil)
		}
	}*/
	return fails
}
