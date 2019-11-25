package core

import (
	"../aws_SDK_wrap"
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MASTER_ADDRESS_PUBLISH_S3_KEY = "MASTER_ADDRESS"

//////////MASTER SIDE
type MASTER_CONTROL struct {
	MasterRpc     *MasterRpc
	MasterAddress string
	Workers       WorkersKinds //connected workers
	WorkersAll    []Worker     //list of all workers ref.
	//list of all avaibles  chunks
	MasterData MASTER_STATE_DATA
	////////// master fault tollerant
	StateChan chan uint32
	State     uint32
	pingConn  net.Conn
	/// master replica fields
	FailsAtStart bool //while refreshing rpc connection to workers some fails is detected
	IsReplica    bool
}

type MASTER_STATE_DATA struct {
	AssignedChunkWorkers            map[int][]int
	AssignedChunkWorkersFairShare   map[int][]int
	ChunkIDS                        []int
	Chunks                          CHUNKS //each chunk here has same indexing of chunkIDs
	MapResults                      []MapWorkerArgsWrap
	ReducerRoutingInfos             ReducersRouteInfos
	ReducerSmartBindingsToWorkersID map[int]int
	ReducersBindingsAddr            map[int]string
}

func Init_distribuited_version(control *MASTER_CONTROL, filenames []string, loadChunksToS3 bool) (*aws_SDK_wrap.UPLOADER, []int, error) {
	///initialize data and workers refs on master
	//if loadChunkToS3 is true will be also published master public IP address on S3 bucket so workers can register (from a cli opt)
	// concurrent intialization by go routine and waitgroup

	barrier := new(sync.WaitGroup)
	barrier.Add(2)
	_, uploader := aws_SDK_wrap.InitS3Links(Config.S3_REGION)

	//control.UploaderState=uploader
	assignedPorts := make([]int, 0, 10)
	var err error = nil
	//init workers,letting them register to master, he will populate different workers kind in ordered manner
	go func() {
		err = waitWorkersRegister(&barrier, control, uploader)
		CheckErr(err, false, "workers initialization failed :(")
	}()

	chunks := InitChunks(filenames)    //chunkize filenames
	control.MasterData.Chunks = chunks //save in memory loaded chunks -> they will not be backup in master checkpointing
	if loadChunksToS3 {                //avoid usless aws put waste if chunks are already loaded to S3
		//init chunks loading to storage service
		println("loading chunks of file to S3")
		go loadChunksToChunkStorage(chunks, &barrier, control, uploader)
	} else {
		(*control).MasterData.ChunkIDS = BuildSequentialIDsListUpTo(len(chunks))
		barrier.Add(-1)
	}

	if Config.BACKUP_MASTER {
		pingPort := NextUnassignedPort(Config.PING_SERVICE_BASE_PORT, &assignedPorts, true, true, "udp")
		control.StateChan = make(chan uint32, 5*Config.FAIL_RETRY) //channel to pass current master state to ping answ routine, buffering is to avoid blocking on channel write
		conn, e := PingHeartBitRcv(pingPort, control.StateChan)
		if CheckErr(e, false, "PING SERVICE STARTING ERR") {
			return uploader, assignedPorts, errors.New(e.Error() + err.Error())
		}
		control.pingConn = conn
	}
	////// sync worker init routines
	barrier.Wait()

	println("initialization done\n\n")
	return uploader, assignedPorts, err
}

func loadChunksToChunkStorage(chunks []CHUNK, waitGroup **sync.WaitGroup, control *MASTER_CONTROL, uploader *aws_SDK_wrap.UPLOADER) {
	//chunkize filenames and upload to storage service

	//initialize upload stuff
	uploadAllBarrier := new(sync.WaitGroup)
	uploadAllBarrier.Add(len(chunks))
	bucket := Config.S3_BUCKET
	chunkIDS := BuildSequentialIDsListUpTo(len(chunks))
	errs := make([]error, 0)
	errsMutex := sync.Mutex{}
	println("concurrent S3 UPLOAD OF ", len(chunks), "CHUNKS")
	startTime := time.Now()
	for i, _ := range chunks {
		keyChunk := strconv.Itoa(i)
		go func(barrier **sync.WaitGroup, i int) {
			err := aws_SDK_wrap.UploadDATA(uploader, string(chunks[i]), keyChunk, bucket)
			if err != nil {
				_, _ = fmt.Fprint(os.Stderr, "upload err", err)
				errsMutex.Lock()
				errs = append(errs, err)
				errsMutex.Unlock()
			}
			print(".")
			(*barrier).Done()
		}(&uploadAllBarrier, i)
	}
	uploadAllBarrier.Wait()
	stopTime := time.Now()
	(*control).MasterData.ChunkIDS = chunkIDS
	(*waitGroup).Done()
	println("loaded: ", len(chunks), "CHUNKS approx in: ", stopTime.Sub(startTime).String())
}

func BuildSequentialIDsListUpTo(maxID int) []int {
	list := make([]int, maxID)
	for i := 0; i < maxID; i++ {
		list[i] = i
	}
	return list
}

func waitWorkersRegister(waitGroup **sync.WaitGroup, control *MASTER_CONTROL, uploader *aws_SDK_wrap.UPLOADER) error {
	//setup worker register service tcp port at master
	//wait workers to registry to master up to some configurable time from first registration (deadline set on socket)
	//for flexibility on relay server between Master and Workers, is now mantained old version of Workers IP prepending in reg msgs) <- minimal overhead

	port := Config.WORKER_REGISTER_SERVICE_BASE_PORT
	conn, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: port,
		IP:   net.ParseIP("0.0.0.0"),
	})
	if CheckErr(err, false, "REGISTER SERVICE SOCKET FAIL") {
		return err
	}
	///////publish master address to workers only if setted
	if (*control).MasterAddress != "" && (*control).MasterAddress != "0" {
		masterRegServiceAddr := (*control).MasterAddress + ":" + strconv.Itoa(port)
		err = aws_SDK_wrap.UploadDATA(uploader, masterRegServiceAddr, MASTER_ADDRESS_PUBLISH_S3_KEY, Config.S3_BUCKET)
		if CheckErr(err, false, "") {
			return err
		}
		println("uploaded master registration addresse: ", masterRegServiceAddr)
	}

	workers := WorkersKinds{
		WorkersMapReduce:  make([]Worker, 0, Config.WORKER_NUM_MAP),
		WorkersOnlyReduce: make([]Worker, 0, Config.WORKER_NUM_ONLY_REDUCE),
		WorkersBackup:     make([]Worker, 0, Config.WORKER_NUM_BACKUP_WORKER),
		//// aggreagate worker registration
	}

	var workerConn *net.TCPConn = nil
	var connBuffered *bufio.Reader = nil //used only in relay server version

	workersKindsNums := map[string]int{
		WORKERS_MAP_REDUCE: Config.WORKER_NUM_MAP, WORKERS_ONLY_REDUCE: Config.WORKER_NUM_ONLY_REDUCE, WORKERS_BACKUP_W: Config.WORKER_NUM_BACKUP_WORKER,
		//taking worker kind addresses list
		//WAIT WORKERS TO REGISTER TO COMUNICATED MASTER ADDRESS EXTRACTING ADDRESS FROM PROBE SYNs
		//println("connection from worker : ", workerConn.LocalAddr().String(), "<---", workerConn.RemoteAddr().String())
	}
	var destWorkersContainer *[]Worker //dest variable for workers to init
	id := 0                            //worker id
	var workerAddr string

	for workerKind, numToInit := range workersKindsNums {

		println("initiating: ", numToInit, "of workers kind: ", workerKind)
		if workerKind == WORKERS_MAP_REDUCE {
			destWorkersContainer = &(workers.WorkersMapReduce)
		} else if workerKind == WORKERS_ONLY_REDUCE {
			destWorkersContainer = &(workers.WorkersOnlyReduce)
		} else if workerKind == WORKERS_BACKUP_W {
			destWorkersContainer = &(workers.WorkersBackup)
		}
		for i := 0; i < numToInit; i++ {
			portBuf := make([]byte, len("6666;7777"))
			if Config.REMOTE_SERVER_PORT_FORWARD { //get worker addresses to estamblish rpc connection over a relay connection to unnatted server
				workerConn, err = conn.AcceptTCP()
				if CheckErr(err, false, "invalid connection from local proxy of relay server") {
					goto exit
				}
				connBuffered = bufio.NewReaderSize(workerConn, len("100.100.100.100:6666;7777- "))

				line, isPrefix, err := connBuffered.ReadLine()
				if isPrefix || CheckErr(err, false, "reading line from relay connection") {
					return err
				}
				ip_port_raw := strings.Split(string(line), ADDR_SEPARATOR)
				workerAddr = ip_port_raw[0]
				if !Config.FIXED_PORT && len(ip_port_raw) == 2 {
					portBuf = []byte(ip_port_raw[1])
				}
			} else {
				workerConn, err = conn.AcceptTCP() //else init new connection for each worker (unnatted master)
				if CheckErr(err, false, "worker connection error") {
					if err, ok := err.(*net.OpError); ok && err.Timeout() {
						println("time out")
						goto exit //timeout=> no more worker to accept
					}
					continue //some fail in connection estamblish...skip worker
				}
				workerAddr = strings.Split(workerConn.RemoteAddr().String(), ":")[0]
			}
			if id == 0 { //set timeout to conn only 1 time after first succesfully connection (assuming at least 1 worker will register)
				registerDeadlineTime := time.Duration(Config.WORKERS_REGISTER_TIMEOUT) * time.Second
				err = conn.SetDeadline(time.Now().Add(registerDeadlineTime))
				println("Worker Registration will close at ", time.Now().Add(registerDeadlineTime).String())
				CheckErr(err, true, "connection timeout err")
			}

			portPing := Config.PING_SERVICE_BASE_PORT
			portControlRpc := Config.CHUNK_SERVICE_BASE_PORT
			if !Config.FIXED_PORT {
				if !Config.REMOTE_SERVER_PORT_FORWARD {
					//TODO SAME CODE CAN BE APPLIED IF WORKER ALWAYS SEND REGISTRATION WITH NEWLINES
					readed := 0
					rd := 0
					lastReaded := ""
					for readed < len(portBuf) && err != io.EOF && lastReaded != PORT_TERMINATOR {
						rd, err = workerConn.Read(portBuf[readed:])
						if err != io.EOF && CheckErr(err, false, "WORKER INIT ERROR") {
							return err
						}
						readed += rd
						lastReaded = string(portBuf[readed-1])
					}
				} else {
					portBuf = portBuf[:len(portBuf)-1] //remove explicitly terminator in remote rely server case
				}
				ports := strings.Split(string(portBuf), PORT_SEPARATOR)
				portControlRpc, _ = strconv.Atoi(ports[0])
				portPing, _ = strconv.Atoi(ports[1])
			}
			//// init workers tcp rpc connections with timeout
			rpcClientChan := make(chan *rpc.Client, 1)
			go func() {
				cli, err := rpc.Dial(Config.RPC_TYPE, workerAddr+":"+strconv.Itoa(portControlRpc))
				if !CheckErr(err, false, "Dial worker at "+workerAddr) {
					rpcClientChan <- cli
				}
			}()
			var client *rpc.Client
			select {
			case client = <-rpcClientChan:
			case <-time.After(time.Duration(Config.WORKER_DIAL_TIMEOUT) * time.Second):
				err = errors.New("DIAL TIMEDOUT FOR WORKER AT " + workerAddr)
			}
			if CheckErr(err, false, "dialing connected worker errd") {
				if !Config.REMOTE_SERVER_PORT_FORWARD || true {
					_ = workerConn.Close()
				}
				continue
			}
			println("Connected to Worker:", id, "at: ", workerAddr, "controPort", portControlRpc, "ping port", portPing)
			newWorker := Worker{
				Address:         workerAddr,
				PingServicePort: portPing,
				Id:              id,
				State: WorkerStateMasterControl{
					ChunksIDs: make([]int, 0, 5),
					ControlRPCInstance: WorkerIstanceControl{
						Port:   portControlRpc,
						Kind:   CONTROL,
						Client: (*CLIENT)(client),
					},
					Failed: false,
				},
			}
			*destWorkersContainer = append(*destWorkersContainer, newWorker)
			if !Config.REMOTE_SERVER_PORT_FORWARD {
				_ = workerConn.Close()
			}
			id++
		}
	}
exit:
	//check eventual init errors accumulated
	err = CheckAndSolveInitErr(&workers)
	//build all worker ref
	workersAll := append(workers.WorkersMapReduce, workers.WorkersOnlyReduce...)
	workersAll = append(workersAll, workers.WorkersBackup...)
	(*control).Workers = workers
	(*control).WorkersAll = workersAll

	(*waitGroup).Done()
	return err
}

func CheckAndSolveInitErr(workersKinds *WorkersKinds) error {
	//check for failed workers inspecting configuration file for workers nums
	//substitute failed workers with backup workers
	//return false if eventual errors are unsolvable (needed more backup workers)

	failedWorkers := 0
	maxTollerableFails := Config.WORKER_NUM_BACKUP_WORKER

	//check backup workres
	expectedNumWorkers := Config.WORKER_NUM_BACKUP_WORKER
	actualNumWorkers := len(workersKinds.WorkersBackup)
	fails := expectedNumWorkers - actualNumWorkers
	if fails > 0 {
		failedWorkers += fails
		maxTollerableFails -= fails
	}
	//check mapReduceWorkers
	expectedNumWorkers = Config.WORKER_NUM_MAP
	actualNumWorkers = len(workersKinds.WorkersMapReduce)
	fails = expectedNumWorkers - actualNumWorkers
	if fails > 0 {
		failedWorkers += fails
		maxTollerableFails -= fails
	}
	if maxTollerableFails < 0 {
		return errors.New("WORKERS INIT ERROR") //too few backup workers
	}
	for i := 0; i < fails; i++ {
		workersKinds.WorkersMapReduce = append(workersKinds.WorkersMapReduce, workersKinds.WorkersBackup[i])
	}
	workersKinds.WorkersBackup = workersKinds.WorkersBackup[:len(workersKinds.WorkersBackup)-fails]
	//check only reduce workers
	expectedNumWorkers = Config.WORKER_NUM_ONLY_REDUCE
	actualNumWorkers = len(workersKinds.WorkersOnlyReduce)
	fails = expectedNumWorkers - actualNumWorkers
	if fails > 0 {
		failedWorkers += fails
		maxTollerableFails -= fails
	}
	if maxTollerableFails < 0 {
		return errors.New("WORKERS INIT ERROR") //too few backup workers
	}
	for i := 0; i < fails; i++ {
		workersKinds.WorkersOnlyReduce = append(workersKinds.WorkersOnlyReduce, workersKinds.WorkersBackup[i])
	}
	workersKinds.WorkersBackup = workersKinds.WorkersBackup[:len(workersKinds.WorkersBackup)-fails]
	println("substituted all failed workers at initialization, residue backup workers :", maxTollerableFails)
	return nil
}

////////// WORKER SIDE
const ADDR_SEPARATOR = ":"  // for flexible port assignement worker will comunicate during registration CONTROL_RPC_PORT;PING_SERVICE_PORT
const PORT_SEPARATOR = ";"  // for flexible port assignement worker will comunicate during registration CONTROL_RPC_PORT;PING_SERVICE_PORT
const PORT_TERMINATOR = "-" // for flexible port assignement worker will comunicate during registration CONTROL_RPC_PORT;PING_SERVICE_PORT

func InitWorker(worker *Worker_node_internal, stopPingChan chan bool, downloader *aws_SDK_wrap.DOWNLOADER) ([]int, error) {
	////// start ping service and initialize worker
	assignedPorts := make([]int, 0, 5)
	/// init worker struct
	pingPort := NextUnassignedPort(Config.PING_SERVICE_BASE_PORT, &assignedPorts, true, true, "udp")                  //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
	controlRpcPort := NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &assignedPorts, true, true, Config.RPC_TYPE) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS

	_, _ = fmt.Fprint(os.Stderr, controlRpcPort, pingPort)

	*worker = Worker_node_internal{
		WorkerChunksStore: WorkerChunks{
			Mutex:  sync.Mutex{},
			Chunks: make(map[int]CHUNK),
		},
		IntermediateDataAggregated: AggregatedIntermediateTokens{
			ChunksSouces:                 make([]int, 0, 5),
			PerReducerIntermediateTokens: make([]map[string]int, Config.ISTANCES_NUM_REDUCE),
		},
		Instances:        make(map[int]WorkerInstanceInternal),
		MapperInstances:  make(map[int]MapperIstanceStateInternal),
		ReducersClients:  make([]*rpc.Client, Config.ISTANCES_NUM_REDUCE),
		ReducerInstances: make([]ReducerIstanceStateInternal, 0, Config.ISTANCES_NUM_REDUCE),
		ExitChan:         make(chan bool),
		StateChan:        make(chan uint32, 5),
		PingPort:         pingPort,
		Downloader:       downloader,
	}
	_, err := PingHeartBitRcv(pingPort, worker.StateChan) //heart bit monitoring service, receive end
	if CheckErr(err, false, "worker init ping service") {
		return assignedPorts, err
	}
	if Config.BACKUP_MASTER {
		(*worker).cacheLock = sync.Mutex{}
	}
	err, _ = InitRPCWorkerIstance(nil, controlRpcPort, CONTROL, worker)
	if CheckErr(err, false, "control rpc init on worker failed") {
		return assignedPorts, err
	}
	return assignedPorts, nil
}

const masterADDR_PUBLIC = "37.116.178.139:6000"
const masterADDR_LOCAL = "127.0.0.1:6000"

func RegisterToMaster(downloader *aws_SDK_wrap.DOWNLOADER, portsComunication string) (string, error) {
	//get master published address from s3
	//register as new worker to master
	//if setted flexible port assignement for worker servieces (control rpc instance and ping service) comunicate to master portsComunication

	////////fetch master address
	var masterAddressStr string
	var err error
	if Config.LOCAL_VERSION {
		masterAddressStr = masterADDR_LOCAL

	} else {
		masterAddressStr, err = GetMasterAddr(downloader, Config.S3_BUCKET, MASTER_ADDRESS_PUBLISH_S3_KEY)
		if CheckErr(err, false, "") {
			return "", err
		}
	}
	println("fetched master address for my registration: ", masterAddressStr)

	//register to master
	conn, err := net.Dial("tcp", masterAddressStr)
	if CheckErr(err, true, "dial error") {
		return "", err
	}
	defer conn.Close()
	if !Config.FIXED_PORT {
		_, err = conn.Write([]byte(portsComunication)) //err not nil if wrt < len
		if CheckErr(err, false, "\n\n\n\n\n\ncomunicating ping port to master") {
			return "", err
		}
	}
	masterAddressStr = strings.Split(conn.RemoteAddr().String(), ":")[0]
	println("registered to master", portsComunication)
	return masterAddressStr, nil
}

func GetMasterAddr(downloader *aws_SDK_wrap.DOWNLOADER, bucketKey string, masterAddrKey string) (string, error) {
	//get master address
	masterAddress := make([]byte, len("255.255.255.255:9696"))
	for v := 0; v < len(masterAddress); v++ { //GO memset (xD)
		masterAddress[v] = 0
	}
	err := aws_SDK_wrap.DownloadDATA(downloader, Config.S3_BUCKET, MASTER_ADDRESS_PUBLISH_S3_KEY, &masterAddress, false)
	if CheckErr(err, false, "master addr fetch err") {
		return "", err
	}
	masterAddressStr := string(masterAddress)
	i := 0
	for i = len(masterAddress) - 1; i >= 0; i-- { //eliminate residue part from downloaded string
		if masterAddress[i] != 0 { //enough to go back until !0
			break
		}
	}
	masterAddressStr = masterAddressStr[:i+1]
	return masterAddressStr, nil
}

func MasterAddressFetch() string {

	return ""
}
