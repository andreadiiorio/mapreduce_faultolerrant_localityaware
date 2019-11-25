package main

import (
	"../aws_SDK_wrap"
	"../core"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

/*
Worker, in distribuited version, simply initialize an RPC server for control operation ,get Master public address, register
then will simply execute commands ordered by the Master
Worker will follow master order by a Control RPC that can be used for the MAP and Reducer rpc server placement on the node
	(that instance will interact with other mappers and return final data to master)
To Test Fault tolerance, random worker will select if die and after what time,
	fully configurable by config.json

*/

var WorkersNodeInternal_localVersion []core.Worker_node_internal //for each local simulated worker indexed by his own id -> intenal state
var WorkersNodeInternal core.Worker_node_internal                //worker nod ref distribuited version

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	core.Config = new(core.Configuration)
	core.ReadConfigFile(core.CONFIGFILEPATH, core.Config)
	stopPingService := make(chan bool, 1)
	myIp := core.ShellCmdWrapGetIp()
	println(myIp, "\t", core.Config.REMOTE_SERVER_PORT_FORWARD)
	println("usage for non default setting:  Schedule Random Crush")
	downloader, _ := aws_SDK_wrap.InitS3Links(core.Config.S3_REGION)
	if core.Config.UPDATE_CONFIGURATION_S3 { //read config file from S3 on argv flag setted
		//download config file from S3
		const INITIAL_CONFIG_FILE_SIZE = 4096
		buf := make([]byte, INITIAL_CONFIG_FILE_SIZE)
		err := aws_SDK_wrap.DownloadDATA(downloader, core.Config.S3_BUCKET, core.CONFIGFILENAME, &buf, false)
		core.CheckErr(err, true, "config file read from S3 error")
		core.DecodeConfigFile(strings.NewReader(string(buf)), core.Config) //decode downloaded config file
	}
	assignedPorts, err := core.InitWorker(&WorkersNodeInternal, stopPingService, downloader)
	core.GenericPrint(assignedPorts, "")
	core.CheckErr(err, true, "worker init error")
	addressesToComunicate := ""
	if !core.Config.FIXED_PORT {
		addressesToComunicate = strconv.Itoa(WorkersNodeInternal.ControlRpcInstance.Port) + core.PORT_SEPARATOR + strconv.Itoa(WorkersNodeInternal.PingPort) + core.PORT_TERMINATOR
		if core.Config.REMOTE_SERVER_PORT_FORWARD {
			addressesToComunicate = myIp + core.ADDR_SEPARATOR + strconv.Itoa(WorkersNodeInternal.ControlRpcInstance.Port) + core.PORT_SEPARATOR + strconv.Itoa(WorkersNodeInternal.PingPort) + core.PORT_TERMINATOR + "\n"
		}
	}
	masterAddr, err := core.RegisterToMaster(downloader, addressesToComunicate)
	core.CheckErr(err, true, "master register err")
	WorkersNodeInternal.MasterAddr = masterAddr

	//fault simulation
	isUnluckyWorker := false //worker to crush
	if len(os.Args) < 2 {
		//no crush flag given, gen with random probability in respect with workers to crush
		totalNumWorkers := core.Config.WORKER_NUM_ONLY_REDUCE + core.Config.WORKER_NUM_BACKUP_WORKER + core.Config.WORKER_NUM_MAP
		crushProbability := float64(core.Config.SIMULATE_WORKERS_CRUSH_NUM) / float64(totalNumWorkers)
		isUnluckyWorker = core.RandomBool(crushProbability, 4)
	} else {
		crushArg := strings.ToUpper(os.Args[1])
		isUnluckyWorker = strings.Contains(crushArg, "TRUE")
	}

	if core.Config.SIMULATE_WORKERS_CRUSH && isUnluckyWorker {
		core.SimulateCrush(core.Config.SIMULATE_WORKER_CRUSH_BEFORE_MILLISEC, core.Config.SIMULATE_WORKER_CRUSH_AFTER_MILLISEC)
	}
	waitEndMR(stopPingService)
	os.Exit(0)
}

func waitEndMR(stopPing chan bool) { //// wait end of Map Reduce
	<-WorkersNodeInternal.ExitChan ///wait worker end setted by the master
	_ = WorkersNodeInternal.ControlRpcInstance.ListenerRpc.Close()
	//for _, instance := range WorkersNodeInternal.Instances {
	//	_ = instance.ListenerRpc.Close()
	//}
	//chanOut := valValue.Interface().(int)
	println("ended worker")
	//_ = worker.PingConnection.Close()
	stopPing <- true
	time.Sleep(1)
}
func waitWorkersEnd(stopPing chan bool) { ////local version
	chanSet := []reflect.SelectCase{}
	for _, worker := range WorkersNodeInternal_localVersion {
		chanSet = append(chanSet, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(worker.ExitChan),
		})
	}
	workersToWait := len(WorkersNodeInternal_localVersion)

	for i := 0; i < workersToWait; i++ {
		//<-worker.ExitChan
		endedWorkerID, chanOut, _ := reflect.Select(chanSet)
		//worker := WorkersNodeInternal_localVersion[endedWorkerID]
		//_ = worker.ControlRpcInstance.ListenerRpc.Close()
		//for _, instance := range worker.Instances {
		//	_ = instance.ListenerRpc.Close()
		//}
		//chanOut := valValue.Interface().(int)
		println("ended worker: ", endedWorkerID, reflect.ValueOf(chanOut).Interface())
	}
	//_ = worker.PingConnection.Close()
	stopPing <- true //quit all ping routine

}
