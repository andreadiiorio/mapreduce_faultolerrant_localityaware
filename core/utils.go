package core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

//////////// FLEX CONFIGURATION
type ConfigInterface interface {
	printFields()
}
type Configuration struct {
	SORT_FINAL               bool   //sort final file (extra computation)
	LOCAL_VERSION            bool   //use function for local deply
	ISTANCES_NUM_REDUCE      int    //number of reducer to istantiate
	WORKER_NUM_ONLY_REDUCE   int    //num of worker node that will exec only 1 reduce istance
	WORKER_NUM_MAP           int    //num of mapper to istantiate
	WORKER_NUM_BACKUP_WORKER int    //num of backup workers for crushed workers
	WORKER_NUM_BACKUP_MASTER int    //num of backup masters
	RPC_TYPE                 string //tcp or http
	// main rpc services base port (other istances on same worker will have progressive port
	CHUNK_SERVICE_BASE_PORT  int
	MAP_SERVICE_BASE_PORT    int
	REDUCE_SERVICE_BASE_PORT int
	MASTER_BASE_PORT         int

	// main loadBalacing vars
	MAX_REDUCERS_PER_WORKER int
	// main replication vars
	CHUNKS_REPLICATION_FACTOR                int
	CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS int
	CHUNK_SIZE                               int64
}

func (config *Configuration) printFields() {
	ReflectionFieldsGet(config)
}

//support variable for dinamic generated addresses read
type WorkerAddresses struct { //refs to up&running workers
	WorkersMapReduce  []string
	WorkersOnlyReduce []string
	WorkersBackup     []string
}

func (addrs *WorkerAddresses) printFields() {
	ReflectionFieldsGet(addrs)
}

//shared configuration
var Config *Configuration

var Addresses *WorkerAddresses //global configuration
const (
	CONFIGFILENAME         = "configurations/config.json"
	ADDRESSES_GEN_FILENAME = "configurations/addresses.json"
	OUTFILENAME            = "finalTokens.txt"
)

var FILENAMES_LOCL = []string{"txtSrc/1012-0.txt"}

type Token struct {
	//rappresent Token middle V out from map phase
	K string
	V int //Key occurence on prj 1
}

func CleanUpFiles(files []*os.File) {
	for _, f := range files {
		e := f.Close()
		CheckErr(e, true, "")
	}
}

func InitChunks(filenames []string) []CHUNK { //fast concurrent file read for chunk generation
	/*
		initialize chunk structure ready to be assigned to map workers
		files will be readed in multiple threads and totalsize will be divided in fair chunks sizes
		eventually the size of the reminder of division for assignment will be assigned to last chunk
	*/
	openedFiles := make([]*os.File, len(filenames))
	fmt.Println("---start chunkization---")
	filesData := make([]string, len(filenames))
	barrierRead := new(sync.WaitGroup)
	barrierRead.Add(len(filenames))
	var totalWorkSize int64 = 0
	//////	chunkize files
	for i, filename := range filenames { //evaluting total work size for fair assignement
		f, err := os.Open(filename)
		CheckErr(err, true, "")
		openedFiles[i] = f
		go func(barrierRead **sync.WaitGroup, destData *string) { //read all files in separated threads
			allbytes, err := ioutil.ReadAll(bufio.NewReader(f))
			CheckErr(err, true, "")
			*destData = string(allbytes)
			(*barrierRead).Done()
			runtime.Goexit()
		}(&barrierRead, &filesData[i])
		fstat, err := f.Stat()
		CheckErr(err, true, "")
		totalWorkSize += fstat.Size()
	}
	barrierRead.Wait() //wait read data end in all threads
	defer closeFileLists(openedFiles)

	var chunkSize int64
	var reminder int64
	var numChunk int
	fixedChunkSize := Config.CHUNK_SIZE > 0
	// chunk num and size based on configuration
	if fixedChunkSize { //fixed chunk size
		chunkSize = Config.CHUNK_SIZE
		numChunk = int(totalWorkSize / chunkSize)
		reminder = totalWorkSize % chunkSize
		if reminder > 0 {
			numChunk++
		}
	} else { //fixed chunk num
		numChunk = Config.WORKER_NUM_MAP
		chunkSize = int64(totalWorkSize / int64(numChunk)) //avg like chunk size
		reminder = int64(totalWorkSize % int64(numChunk))  //assigned to first Worker
	}
	filesChunkized := make([]CHUNK, numChunk)
	allStr := strings.Join(filesData, "")

	var low, high int64
	for x := 0; x < len(filesChunkized); x++ {
		low = chunkSize * int64(x)
		high = Min(chunkSize*int64(x+1), int64(len(allStr))) //fixed chunk size and positive reminder => last extra chunk smaller
		filesChunkized[x] = CHUNK(allStr[low:high])
	}
	if !fixedChunkSize && reminder > 0 {
		filesChunkized[len(filesChunkized)-1] = CHUNK(allStr[low : high+reminder]) //last Worker get bigger chunk
	}
	return filesChunkized
}

func closeFileLists(files []*os.File) {
	for _, file := range files {
		err := file.Close()
		CheckErr(err, true, "closing files stage...")
	}
}

//// HASHING KEY FUNCs
func HashKeyReducerSum(key string, maxIDOut int) int {
	//simply hash string to int in [0,maxIDOut) by sum chars and %
	//for the given key string will return the ID of dest reducer
	const EXTRASHUFFLE = 96 //extra shuffle in hash func
	sum := 0
	for c := range key {
		sum += c
	}
	sum += EXTRASHUFFLE
	return sum % maxIDOut
}

//// SORT_FINAL SUPPORT FUNCTION
// FOR TOKEN LIST
type TokenSorter struct {
	//rappresent Token middle V out from map phase
	tokens []Token
	//by func(tks1,tks2 Token)	//sorting function ... default by Key builtin sort func
}

func (t TokenSorter) Len() int {
	return len(t.tokens)
}
func (t TokenSorter) Swap(i, j int) {
	t.tokens[i], t.tokens[j] = t.tokens[j], t.tokens[i]
}
func (t TokenSorter) Less(i, j int) bool {
	return t.tokens[i].V < t.tokens[j].V
}

////  ROUTING COSTS
type RoutingCostsSorter struct {
	routingCosts []TrafficCostRecord
}

func (r RoutingCostsSorter) Len() int {
	return len(r.routingCosts)
}
func (r RoutingCostsSorter) Swap(i, j int) {
	r.routingCosts[i], r.routingCosts[j] = r.routingCosts[j], r.routingCosts[i]
}
func (r RoutingCostsSorter) Less(i, j int) bool {
	return r.routingCosts[i].RoutingCost < r.routingCosts[j].RoutingCost
}

/////	HEARTBIT FUNCS		/////
func PingHeartBitRcv() {
	//TODO PING receve
}
func PingHeartBitSnd() {
	//TODO PING send
	//TODO CHEAP TRIGGER ON NO ANSW
}

////	INSTANCES FUNCs
func GetMaxIdWorkerInstances(workerInstances *map[int]WorkerInstanceInternal) int {
	maxId := 0
	for id, _ := range *workerInstances {
		if id > maxId {
			maxId = id //updateMaxId
		}
	}
	return maxId
}

/// OTHER
func CheckErrs(errs []error, fatal bool, supplementMsg string) bool {

	for _, e := range errs {
		if CheckErr(e, fatal, supplementMsg) == true {
			return true
		}
	}
	return false
}
func CheckErr(e error, fatal bool, supplementMsg string) bool {
	//check error, exit if fatal is true
	//return bool, true if err is not nil
	if e != nil {
		baseMsg := e.Error()
		if fatal == true {
			log.Fatal("\n\n"+baseMsg+supplementMsg, e)
		} else {
			log.Println("\n\n"+baseMsg+supplementMsg, e)
		}
		return true
	}
	return false
}
func Max(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
func Min(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func SerializeToFile(defTokens []Token, filename string) {
	/////	SERIALIZE RESULT TO FILE
	n := 0
	lw := 0
	encodeFile, err := os.Create(filename)
	CheckErr(err, true, "")
	_, err = encodeFile.Seek(0, 0)
	CheckErr(err, true, "")
	defer encodeFile.Close()
	for _, tk := range defTokens {
		line := fmt.Sprint(tk.K, "->", tk.V, "\r\n")
	write:
		n, err = encodeFile.WriteString(line[lw:])
		CheckErr(err, true, "")
		if n < len(line) {
			lw += n
			fmt.Println("write short...")
			goto write
		}
		lw = 0
	}
}
func ListOfDictCumulativeSize(dictList []map[int]int) int {
	cumulativeSum := 0
	for _, dict := range dictList {
		cumulativeSum += len(dict)
	}
	return cumulativeSum
}
func DictsNestedCumulativeSize(dictsNestes map[int]map[int]int) int {
	cumulativeSum := 0
	for _, dict := range dictsNestes {
		cumulativeSum += len(dict)
	}
	return cumulativeSum
}

func ReadConfigFile(configFilePath string, destVar ConfigInterface) {
	f, err := os.Open(configFilePath)
	CheckErr(err, true, "config file open")
	defer f.Close()
	//configRawStr,err:=ioutil.ReadAll(bufio.NewReader(f))
	decoder := json.NewDecoder(f)
	err = decoder.Decode(destVar)
	CheckErr(err, true, "")

}

func ReflectionFieldsGet(strct interface{}) {
	val := reflect.ValueOf(strct)
	values := make(map[string]interface{}, val.NumField())
	metaTypes := val.Type()
	for i := 0; i < val.NumField(); i++ {
		values[metaTypes.Field(i).Name] = val.Field(i).Interface()
	}

	fmt.Println(values)

}
func GenericPrint(slice interface{}) bool {
	sv := reflect.ValueOf(slice)

	for i := 0; i < sv.Len(); i++ {
		fmt.Printf("%d\t", sv.Index(i).Interface())
	}
	fmt.Printf("\n\n")
	return false
}

//ports
func CheckPortAvaibility(port int) (status bool) {

	// Concatenate a colon and the port
	host := ":" + strconv.Itoa(port)
	errs := make([]error, 2)
	// Try to create a server with the port
	server, err := net.Listen("tcp", host)
	errs = append(errs, err)
	err = server.Close()
	// close the server
	errs = append(errs, err)
	return !CheckErrs(errs, true, "checking port avaibility"+strconv.Itoa(port))

}
