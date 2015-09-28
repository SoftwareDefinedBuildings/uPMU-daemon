package main

import (
	"encoding/binary"
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

const (
	CONNBUFLEN = 1024 // number of bytes we read from the connection at a time
	MAXFILEPATHLEN = 512
	MAXSERNUMLEN = 32
	EXPDATALEN = 757440
	MAXDATALEN = 75744000
)

func roundUp4(x uint32) uint32 {
	return (x + 3) & 0xFFFFFFFC
}

func processMessage(sendid []byte, sernum string, filepath string, data []byte) []byte {
	//TODO
	return make([]byte, 4, 4)
}

func handlePMUConn(conn *net.TCPConn) {
	fmt.Printf("Connected: %s\n", conn.RemoteAddr().String())
	
	defer conn.Close()
	
	/* Stores error on failed read from TCP connection. */
	var err error
	
	/* Stores error on failed write to TCP connection. */
	var erw error
	
	/* The id of a message is 4 bytes long. */
	var sendid []byte
	
	/* The length of the filepath. */
	var lenfp uint32
	/* The length of the filepath, including the padding added so it ends on a word boundary. */
	var lenpfp uint32
	
	/* The length of the serial number. */
	var lensn uint32
	/* The length of the serial number, including the padding added so it ends on a word boundary. */
	var lenpsn uint32
	
	/* The length of the data. */
	var lendt uint32
	
	/* Read from the Connection 1 KiB at a time. */
	var buf []byte = make([]byte, CONNBUFLEN)
	var bpos int
	
	/* INFOBUFFER stores length data from the beginning of the message to get the length of the rest. */
	var infobuffer [16]byte
	var ibindex uint32
	
	/* FPBUFFER stores the filepath data received so far. */
	var fpbuffer []byte = make([]byte, MAXFILEPATHLEN, MAXFILEPATHLEN)
	var fpindex uint32
	var filepath string
	
	/* SNBUFFER stores the part of the serial number received so far. */
	var snbuffer []byte = make([]byte, MAXSERNUMLEN, MAXSERNUMLEN)
	var snindex uint32
	var sernum string
	
	/* DTBUFFER stores the part of the uPMU data received so far.
	   If a file is bigger than expected, we allocate a bigger buffer, specially for that file. */
	var dtbufferexp []byte = make([]byte, EXPDATALEN, EXPDATALEN)
	var dtbuffer []byte = nil
	var dtindex uint32

	/* N stores the number of bytes read in the current read from the TCP connection. */
	var n int
	/* TOTRECV stores the total number of bytes read from the TCP connection for this message. */
	var totrecv uint32
	
	/* The response to send to the uPMU. */
	var resp []byte
	var recvdfull bool
	
	// Infinite loop to keep reading messages until connection is closed
	for {
		ibindex = 0
		fpindex = 0
		snindex = 0
		dtindex = 0
		totrecv = 0
		recvdfull = false
		// Read and process the message
		for !recvdfull {
			n, err = conn.Read(buf)
			bpos = 0
			if totrecv < 16 {
				for ibindex < 16 && bpos < n {
					infobuffer[ibindex] = buf[bpos]
					ibindex++
					bpos++
					totrecv++
				}
				if ibindex == 16 {
					sendid = infobuffer[:4]
					lenfp = binary.LittleEndian.Uint32(infobuffer[4:8])
					lensn = binary.LittleEndian.Uint32(infobuffer[8:12])
					lendt = binary.LittleEndian.Uint32(infobuffer[12:16])
					lenpfp = roundUp4(lenfp)
					lenpsn = roundUp4(lensn)
					if lenfp > MAXFILEPATHLEN {
						fmt.Printf("Filepath length fails sanity check: %v\n", lenfp)
						return
					}
					if lensn > MAXSERNUMLEN {
						fmt.Printf("Serial number length fails sanity check: %v\n", lensn)
						return
					}
					if lendt > MAXDATALEN {
						fmt.Printf("Data length fails sanity check: %v\n", lendt)
						return
					}
					if lendt <= EXPDATALEN {
						dtbuffer = dtbufferexp
					} else {
						dtbuffer = make([]byte, lendt, lendt)
					}
				}
			}
			if totrecv < 16 + lenpfp {
				for fpindex < lenpfp && bpos < n {
					fpbuffer[fpindex] = buf[bpos]
					fpindex++
					bpos++
					totrecv++
				}
				if fpindex == lenpfp {
					filepath = string(fpbuffer[:lenfp])
				}
			}
			if totrecv < 16 + lenpfp + lenpsn {
				for snindex < lenpsn && bpos < n {
					snbuffer[snindex] = buf[bpos]
					snindex++
					bpos++
					totrecv++
				}
				if snindex == lenpsn {
					sernum = string(snbuffer[:lensn])
				}
			}
			if totrecv < 16 + lenpfp + lenpsn + lendt {
				for dtindex < lendt && bpos < n {
					dtbuffer[dtindex] = buf[bpos]
					dtindex++
					bpos++
					totrecv++
				}
				if dtindex == lendt {
					if bpos < n {
						fmt.Printf("WARNING: got %d extra bytes\n", n - bpos)
					}
					// if we've reached this point, we have all the data
					recvdfull = true
					fmt.Printf("Received %s: serial number is %s (%s), length is %v\n", filepath, sernum, "alias not known", lendt)
					resp = processMessage(sendid, sernum, filepath, dtbuffer[:lendt])
					_, erw = conn.Write(resp)
					if erw != nil {
						fmt.Printf("Write to TCP Connection did not complete successfully: %v\n", erw);
						return
					}
				}
			}
			if err != nil {
				fmt.Printf("Read from TCP Connection did not complete successfully: %v\n", err)
				return
			}
		}
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
