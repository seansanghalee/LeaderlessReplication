package main

import (
	"LeaderlessReplication/config"
	"LeaderlessReplication/data"
	"LeaderlessReplication/receiver"
	"LeaderlessReplication/sender"

	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var storage []data.Data // global data storage
var allowedFailures int // N - f
var ackArray []data.Data
var useDisk bool

func printServers(nodes map[string]net.Conn) {
	fmt.Println("Connected Servers:", len(nodes))
	fmt.Println("Servers:")
	for key, value := range nodes {
		fmt.Println("ID:", key)
		fmt.Println("Address:", value)
	}
}

func sendToOtherServers(d data.Data, nodes map[string]net.Conn) {
	for _, c := range nodes {
		sender.UnicastSend(c, d)
	}
}

/*func countAcks(nodes map[string]net.Conn) []data.Data {
	var ackArray []data.Data
	for id, c := range nodes {
		ack := data.Data{}
		receiver.UnicastReceive(c, &ack)
		if ack.Ack == 1 {
			fmt.Println("Received ACK from Server: ", id)
			ackArray = append(ackArray, ack)
		}
	}
	return ackArray
}*/

func serve(c net.Conn, nodes map[string]net.Conn) {
	for {
		ackArray = nil
		d := data.Data{}
		receiver.UnicastReceive(c, &d)
		switch d.ReadOrWrite {
		case 0: //client is reading
			// send all the other servers that we want to read this key
			sendToOtherServers(d, nodes)

			// waits for N - f acks
			// adds the current server's key-value pair to compare
			index := contains(storage, d.Key)
			if index >= 0 {
				ackArray = append(ackArray, storage[index])
			}

			for len(ackArray) < allowedFailures-1 {
			}

			var counter int

			for i := range ackArray {
				if ackArray[i].Exists == 1 {
					counter += 1
				}
			}

			if counter < allowedFailures {
				//notify the client that the provided key does not exist
				d.Ack = 0
				sender.UnicastSend(c, d)
				break
			}

			// check timestamp
			d = ackArray[0]
			t := ackArray[0].Timestamp
			for i := range ackArray {
				newtime := ackArray[i].Timestamp
				if newtime.After(t) {
					// get most recent key value pair
					d = ackArray[i]
				}
			}
			// send it back to the client
			d.Ack = 1
			sender.UnicastSend(c, d)

		case 1: //client is writing
			// write to this server
			index := contains(storage, d.Key)
			if index >= 0 {
				storage[index] = d
			} else {
				storage = append(storage, d)
			}
			// send all the other servers that we want to write this key-value pair
			sendToOtherServers(d, nodes)

			// waits for N - f acks

			for len(ackArray) < allowedFailures-1 {
			}

			if len(ackArray)+1 < allowedFailures {
				d.Ack = 0
			} else {
				d.Ack = 1
			}
			// sends ack back to the client
			sender.UnicastSend(c, d)
		default:
			sender.UnicastSend(c, d)
		}
	}
}

//find in storage array
func contains(s []data.Data, str string) int {
	for idx, v := range s {
		if v.Key == str {
			return idx
		}
	}
	return -1
}

func listenToOtherServers(c net.Conn) {
	for {
		d := data.Data{}
		receiver.UnicastReceive(c, &d)

		if d.Ack == 1 {
			d.ReadOrWrite = -1
			ackArray = append(ackArray, d)
			continue
		}

		switch d.ReadOrWrite {
		case 0: // reading
			//check if you have k,v pair in storage
			//if you have it, send it back
			if contains(storage, d.Key) >= 0 {
				d.ReadOrWrite = -1
				d.Ack = 1
				d.Exists = 1
				sender.UnicastSend(c, d)
			} else {
				d := data.Data{}
				d.Ack = 1
				d.Exists = 0
				d.ReadOrWrite = -1
				sender.UnicastSend(c, d)
			}

		case 1: // writing
			index := contains(storage, d.Key)
			if index >= 0 {
				storage[index] = d
			} else {
				storage = append(storage, d)
			}
			// fmt.Println(storage)
			d.ReadOrWrite = -1
			d.Ack = 1
			sender.UnicastSend(c, d)

		default:
			d.ReadOrWrite = -1
			sender.UnicastSend(c, d)
		}
	}
}

func main() {
	arguments := os.Args // takes in the command argument and identify the ID and the port number

	// checks if host address and port # are provided
	if len(arguments) == 1 {
		fmt.Println("Please provide port")
		return
	} else if len(arguments) == 2 {
		fmt.Println("Please provide disk flag")
	}

	ID, _ := strconv.Atoi(arguments[1])

	if strings.Contains(arguments[2], "disk") {
		useDisk = true

		// For Testing:
		// utils.WriteToFile(string(arguments[1]), "sup", "peace")
		// utils.ReadFromFile(string(arguments[1]), "sup")
	} else {
		useDisk = false
	}

	// reads YAML file and extract information
	c, err := config.ReadConf("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// get the port of this server from config file and ID provided
	var port string

	for i := 0; i < c.NumServers; i++ {
		// fmt.Println(c.Servers[i])
		if c.Servers[i].ID == ID {
			port = c.Servers[i].Port
		}
	}

	fmt.Println("ID: ", ID)

	allowedFailures = c.NumServers - c.NumFailures //sets N-f
	nodes := make(map[string]net.Conn)             // tracks server ID and InetAddress

	// TODO - goroutine to always listen to the incoming dial, connect whenever
	for i := ID + 1; i < c.NumServers; i++ {
		fmt.Println("Listening for server", i, "| Port", c.Servers[i].Port)
		receiver.ServerListen(port, nodes)
	}

	for i := 0; i < ID; i++ {
		fmt.Println("Dialing server:", i)
		sender.Dial(i, ID, c.Servers[i].IP, c.Servers[i].Port, nodes)
	}

	printServers(nodes)

	for _, c := range nodes { // all servers
		go listenToOtherServers(c)
	}

	for {
		l, err := net.Listen("tcp", ":"+port)
		if err != nil {
			fmt.Println(err)
			return
		}

		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		l.Close()

		fmt.Println("Client connected")

		go serve(c, nodes)
		fmt.Println("Serving client")
	}
}
