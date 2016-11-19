package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net"
	"os"
	"time"
	"bufio"
	"strings"
)

var send_semaphore chan *mgo.Session

var aliases map[string]string

var FAILUREMSG = make([]byte, 4, 4)

const (
	CONNBUFLEN = 1024 // number of bytes we read from the connection at a time
	MAXFILEPATHLEN = 512
	MAXSERNUMLEN = 32
	EXPDATALEN = 757440
	MAXDATALEN = 75744000
	MAXCONCURRENTSESSIONS = 16
	TIMEOUTSECS = 30
)

func roundUp4(x uint32) uint32 {
	return (x + 3) & 0xFFFFFFFC
}

type MessageDoc struct {
	Filepath string `json:"name" bson:"name"`
	Data bson.Binary `json:"data" bson:"data"`
	Published bool `json:"published" bson:"published"`
	TimeReceived time.Time `json:"time_received" bson:"time_received"`
	SerialNumber string `json:"time_received" bson:"serial_number"`
}

func processMessage(sendid []byte, sernum string, filepath string, data []byte) []byte {
	var dberr error

	var msgdoc *MessageDoc = &MessageDoc{
		Filepath: filepath,
		Data: bson.Binary{
			Kind: 0x00,
			Data: data,
		},
		Published: false,
		TimeReceived: time.Now().UTC(),
		SerialNumber: sernum,
	}

	var docsel bson.M = bson.M{"serial_number": sernum}
	var updatecmd bson.M = bson.M{"$set": bson.M{"time_received": msgdoc.TimeReceived}}

	var session *mgo.Session = <- send_semaphore
	var upmu_database *mgo.Database = session.DB("upmu_database")
	var received_files *mgo.Collection = upmu_database.C("received_files")
	var latest_times *mgo.Collection = upmu_database.C("latest_times")

	defer func () { send_semaphore <- session }()

	// Insert data
	dberr = received_files.Insert(msgdoc)
	if dberr != nil {
		session.Refresh()
		fmt.Printf("Could not insert file into received_files collection: %v\n", dberr)
		return FAILUREMSG
	}

	// Update latest time
	_, dberr = latest_times.Upsert(docsel, updatecmd)
	if dberr != nil {
		session.Refresh()
		fmt.Printf("Could not update latest_times collection: %v\n", dberr)
		return FAILUREMSG
	}

	// Database was successfully updated
	return sendid
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
	var newsernum string

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
			if bpos < n && totrecv < 16 {
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
					if lenfp != 0 && lenfp > MAXFILEPATHLEN {
						fmt.Printf("Filepath length fails sanity check: %v\n", lenfp)
						return
					}
					if lensn != 0 && lensn > MAXSERNUMLEN {
						fmt.Printf("Serial number length fails sanity check: %v\n", lensn)
						return
					}
					if lendt != 0 && lendt > MAXDATALEN {
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
			if bpos < n && totrecv < 16 + lenpfp {
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
			if bpos < n && totrecv < 16 + lenpfp + lenpsn {
				for snindex < lenpsn && bpos < n {
					snbuffer[snindex] = buf[bpos]
					snindex++
					bpos++
					totrecv++
				}
				if snindex == lenpsn {
					newsernum = string(snbuffer[:lensn])
					if sernum != "" && newsernum != sernum {
						fmt.Printf("WARNING: serial number changed from %s to %s\n", sernum, newsernum)
						fmt.Println("Updating serial number for next write")
					}
					sernum = newsernum
				}
			}
			if bpos < n && totrecv < 16 + lenpfp + lenpsn + lendt {
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
					alias, ok := aliases[sernum]
					if !ok {
						alias = "UNKNOWN"
					}
					recvdfull = true
					fmt.Printf("Received %s: serial number is %s (%s), length is %v\n", filepath, sernum, alias, lendt)
					resp = processMessage(sendid, sernum, filepath, dtbuffer[:lendt])
					_, erw = conn.Write(resp)
					if erw != nil {
						fmt.Printf("Connection lost: %v (write failed: %v)\n", conn.RemoteAddr().String(), erw);
						return
					}
				}
			}
			if err != nil {
				fmt.Printf("Connection lost: %v (reason: %v)\n", conn.RemoteAddr().String(), err)
				return
			}
		}
	}
}

func main() {
	var port *int = flag.Int("p", 1883, "the port at which to accept incoming messages")
	var aliasfile *string = flag.String("a", "", "an alias configuration file")
	var mongoaddr *string = flag.String("m", "localhost:27017", "hostname/port of mongo database")
	flag.Parse()

	aliases = make(map[string]string)

	if *aliasfile != "" {
			file, err := os.Open(*aliasfile)
			if err != nil {
				fmt.Println("Could not file alias file")
				os.Exit(1)
			}
			r := bufio.NewReader(file)
			for {
				ln, _, err := r.ReadLine()
				if err != nil {
					break
				}
				sl := strings.Split(string(ln), "=")
				aliases[strings.TrimSpace(sl[0])] = strings.TrimSpace(sl[1])
			}
	}
	var err error

	var basesession *mgo.Session

	basesession, err = mgo.Dial(*mongoaddr)
	if err != nil {
		fmt.Printf("Could not connect to Mongo DB: %v\n", err)
		os.Exit(1)
	}

	var safetylevel *mgo.Safe = &mgo.Safe{W: 1, WTimeout: 1000 * TIMEOUTSECS, FSync: true}
	basesession.EnsureSafe(safetylevel)

	var c *mgo.Collection = basesession.DB("upmu_database").C("received_files")

	fmt.Println("Verifying that database indices exist...")
	err = c.EnsureIndex(mgo.Index{
		Key: []string{ "serial_number", "ytag", "name" },
	})

	if err == nil {
		err = c.EnsureIndex(mgo.Index{
			Key: []string{ "serial_number", "name" },
		})
	}

	if err != nil {
		fmt.Printf("Could not build indices on Mongo database: %v\nTerminating program...", err)
		os.Exit(1)
	}

	send_semaphore = make(chan *mgo.Session, MAXCONCURRENTSESSIONS)
	for i := 0; i < MAXCONCURRENTSESSIONS; i++ {
		send_semaphore <- basesession.Copy()
	}
	basesession.Close()

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

	fmt.Println("Waiting for incoming connections...")

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
