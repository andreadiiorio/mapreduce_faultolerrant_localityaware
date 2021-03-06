package main

//TODO DEPRECATED
/*
Worker MODULE
worker will be bounded to different ports by adding a costant to Config.PORTBASE
rpc server initialization is terminate on different go routine
will be rysed the max from rpc server for map and reduce
TODO worker rpc server die waiting signal from master on a channel...ask if ok
*/
/*
type WORKER struct {
	port      int
	address   string
	client    *rpc.Client
	terminate chan bool
}

func workersInit(n int) []WORKER {
	//init n worker and return a Worker struct for each rysed worker
	workerRefs := make([]WORKER, n)
	var err error
	for x := 0; x < n; x++ {
		ch := make(chan bool, 1)
		workerRefs[x].terminate = ch
		port := Config.PORTBASE + x
		workerRefs[x].port = port
		address := fmt.Sprint("localhost:", port)
		workerRefs[x].address = address
		go rpcInit(x, &ch) //ryse up worker with buffered channel
		_ = <-ch           //WAIT Worker THREAD ENDED SETUP CALLS BY CHANNEL NOTIFICATION
		workerRefs[x].client, err = rpc.Dial("tcp", address)
		if err != nil {
			log.Fatal("Error in dialing: ", err)
		}
	}
	return workerRefs
}
func rpcInit(off_port int, done *chan bool) {
	//INIT AN RPC SERVER UNDER PORT BASE + off_port
	//END RPC SERVER ON MASTER NOTIFY ON DONE CHANNEL

	//Create an instance of structs which implements map and reduce interfaces
	map_ := new(_map)
	reduce_ := new(_reduce)

	//REGISTER MAP AND REDUCE METHODS
	// Only structs which implement $* interface are allowed to register themselves
	server := rpc.NewServer()

	err := server.RegisterName("Map", map_)
	if err != nil {
		log.Println("Format of service Map is not correct: ", err)
	}
	err = server.RegisterName("Reduce", reduce_)
	if err != nil {
		log.Println("Format of service Reduce is not correct: ", err)
	}
	port := Configuration.PORTBASE + off_port

	// Listen for incoming tcp packets on port by specified offset of port base.
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	if e != nil {
		log.Println("Listen error on port ", port, e)
	}

	go server.Accept(l) // a new thread is blocked serving rpc requests

	*done <- true //signal worker  creator that: rpc service UP&RUNNING
	_ = <-*done   //terminate channel read unblock when master notify worker to end
	//_:=l.Close()           //TODO will unblock rpc requests handler routine
	//runtime.Goexit()    //routine end here
}*/

//old version of init worker differentiating map or reduce, map on barrier
////RPC FOR MAP AND REDUCE, REDUCE BLOCKED ON ACCEPT, MAP SERVE ONLY ONE CALL
//if barrier==nil   {			////REDUCE case
//	server.Accept(l) 		//blocked until listener error
//	//TODO REDUCE unblock on terminate work...by chan is ok for prj?
//} else {					////MAP case
//	//map case => block only for first call, then exit
//	conn, err := l.Accept()
//	defer conn.Close()
//	if err != nil {
//		log.Print("rpc.Serve: accept:", err.Error())
//		return
//	}
//	server.ServeConn(conn) 	//block until connected client hangs up
//	fmt.Println("SERVED RPC REQ")
//	barrier.Done()			//notify master map work has finished
//	}
//runtime.Goexit()		//thread exit on terminate work
