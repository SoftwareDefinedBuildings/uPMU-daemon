package main

import (
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"net"
	"os"
)

var (
	session *mgo.Session
	upmu_database *mgo.Database
	received_files *mgo.Collection
	latest_times *mgo.Collection
	warnings *mgo.Collection
	warnings_summary *mgo.Collection
	DIRDEPTH int
)

func handlePMUConn(conn *net.TCPConn) {
	fmt.Println("Got an incoming TCP Connection!")
	var err error = conn.Close()
	if err == nil {
		fmt.Println("Successfully closed TCP Connection")
	} else {
		fmt.Printf("Could not close TCP Connection: %v\n", err)
	}
}

func main() {
	var depth *int = flag.Int("d", 4, "the depth of the files in the directory structure being sent (top level is at depth 0)")
	var port *int = flag.Int("p", 1883, "the port at which to accept incoming messages")
	flag.Parse()
	DIRDEPTH = *depth

	var err error
	
	session, err = mgo.Dial("localhost:27017")
	if err != nil {
		fmt.Printf("Could not connect to Mongo DB: %v\n", err)
		os.Exit(0)
	}
	
	upmu_database = session.DB("upmu_database")
	received_files = upmu_database.C("received_files")
	latest_times = upmu_database.C("latest_times")
	warnings = upmu_database.C("warnings")
	warnings_summary = upmu_database.C("warnings_summary")
	
	var bindaddr *net.TCPAddr
	var listener *net.TCPListener
	
	bindaddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%v", *port))
	if err != nil {
		fmt.Printf("Could not resolve address to bind TCP server socket: %v\n", err)
		os.Exit(0)
	}
	listener, err = net.ListenTCP("tcp", bindaddr)
	if err != nil {
		fmt.Printf("Could not create bound TCP server socket: %v\n", err)
		os.Exit(0)
	}
	
	var upmuconn *net.TCPConn
	for {
		upmuconn, err = listener.AcceptTCP()
		if err == nil {
			go handlePMUConn(upmuconn)
		} else {
			fmt.Printf("Could not accept incoming TCP connection: %v\n", err)
		}
	}
}
