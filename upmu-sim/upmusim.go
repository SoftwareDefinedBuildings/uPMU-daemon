package main

import (
    "bytes"
    "encoding/binary"
	"fmt"
    "math"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

    "github.com/SoftwareDefinedBuildings/sync2_quasar/upmuparser"
)

func roundUp4(x uint32) uint32 {
	return (x + 3) & 0xFFFFFFFC
}

//simulate a PMU waiting interval seconds between files
func simulatePmu(target string, serial int64, interval int64) {
    var sendid uint32 = 0

	//Add jitter to simulation
	time.Sleep(time.Duration(float64(interval)*rand.Float64()*1000.0) * time.Millisecond)

	//Later we can improve this so that multiple runs of the simulator
	//with interval < 120 do not overlap if restarted immediately.
	//for now I don't care. Round it to 1 second
	startTime := (time.Now().UnixNano() / 1000000000) * 1000000000

	//Outer for loop. Do reconnections
	for {
	//reconnect:
		//Reconnect to server
		serial := fmt.Sprintf("P%d", serial)
		fmt.Printf("Connecting virtual PMU %s to server %s\n", serial, target)

		conn, err := net.Dial("tcp", target)
		if err != nil {
			fmt.Printf("Could not connect to receiver: %v\n", err)
			time.Sleep(time.Duration(interval) * time.Second)
			continue
		}

		for {
			//Inner loop, for each file
			startTime += 120 * 1000 * 1000 * 1000
			var blob []byte = generateFile(startTime / 1000000000)

            // Filepath for this file...
            var filepath string = fmt.Sprintf("/simulation/file%v.dat", sendid)

            var lenfp uint32 = uint32(len(filepath))
            var lensn uint32 = uint32(len(serial))
            var lendt uint32 = uint32(len(blob))

            var lenpfp = roundUp4(lenfp)
            var lenpsn = roundUp4(lensn)

			//Send the blob to the receiver
            var header_buf []byte = make([]byte, 16 + lenpfp + lenpsn)
            var sendid_buf []byte = header_buf[0:4]
            var lenfp_buf []byte = header_buf[4:8]
            var lensn_buf []byte = header_buf[8:12]
            var lendt_buf []byte = header_buf[12:16]

            binary.LittleEndian.PutUint32(sendid_buf, sendid)
            binary.LittleEndian.PutUint32(lenfp_buf, lenfp)
            binary.LittleEndian.PutUint32(lensn_buf, lensn)
            binary.LittleEndian.PutUint32(lendt_buf, lendt)

            copy(header_buf[16:16+lenpfp], filepath)
            copy(header_buf[16+lenpfp:], serial)

            n, err := conn.Write(header_buf)
            if n != len(header_buf) || err != nil {
                panic("TCP write failed on header")
            }
            n, err = conn.Write(blob)
            if n != len(blob) || err != nil {
                panic("TCP write failed on file")
            }

            var resp_buf []byte = make([]byte, 4)

            totalread := 0
            for totalread < 4 {
                n, err = conn.Read(resp_buf[totalread:])
                if err != nil {
                    panic("Could not get confirmation of receipt")
                }
                totalread += n
            }

            var resp uint32 = binary.LittleEndian.Uint32(resp_buf)
            if resp != sendid {
                fmt.Printf("Received improper confirmation of receipt: got %v, expected %v\n", resp, sendid)
            }

            sendid++

			//Increment our stats
			atomic.AddInt64(&sent, 1)
			//Wait INTERVAL before doing next file
			time.Sleep(time.Duration(interval) * time.Second)
		}
	}
}

func generateFile(startTime int64) []byte {
	//generate a 120 second long file starting from startTime in SECONDS
	//and return it as a byte array
    var buffer bytes.Buffer
    for j := 0; j < 120; j++ {
        var data upmuparser.Upmu_one_second_output_standard = generateSecond(startTime + int64(j))
        binary.Write(&buffer, binary.LittleEndian, &data)
    }

    return buffer.Bytes()
}

func generateSecond(startTime int64) upmuparser.Upmu_one_second_output_standard {
    var time time.Time = time.Unix(startTime, 0)
    var data upmuparser.Upmu_one_second_output_standard
    data.Data.Sample_interval_in_milliseconds = 1000.0 / 120.0
    data.Data.Timestamp[0] = int32(time.Year())
    data.Data.Timestamp[1] = int32(time.Month())
    data.Data.Timestamp[2] = int32(time.Day())
    data.Data.Timestamp[3] = int32(time.Hour())
    data.Data.Timestamp[4] = int32(time.Minute())
    data.Data.Timestamp[5] = int32(time.Second())

    /* Fill in the actual data */
    for i := 0; i < 120; i++ {
        data.Data.L1_e_vector_space[i].Fundamental_magnitude_volts = float32(math.Sin(float64(i) * 2.0 * math.Pi / 120))
    }

    return data
}
