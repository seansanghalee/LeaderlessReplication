package main

import (
	"LeaderlessReplication/config"
	"LeaderlessReplication/data"
	"LeaderlessReplication/receiver"
	"LeaderlessReplication/sender"
	"LeaderlessReplication/utils"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Storage is a global data storage that stores key-value pairs with its timestamp
type Storage struct {
	mu sync.Mutex
	s  []data.Data
}

// AckArray is a gloabl data array that is used to perform most recent read and write calculations
type AckArray struct {
	mu sync.Mutex
	a  []data.Data
}

var storage Storage
var ackArray AckArray
var allowedFailures int // N - f
var useDisk bool
var serverID string

func printServers(nodes map[string]net.Conn) {
	fmt.Println("----- Connected Servers:", len(nodes), "-----")
	for key, value := range nodes {
		fmt.Println("ID:", key)
		fmt.Println("Address:", value)
	}
	fmt.Println("--------------------------------")
}

func sendToOtherServers(d data.Data, nodes map[string]net.Conn) {
	for _, c := range nodes {
		sender.UnicastSend(c, d)
	}
}

func serve(c net.Conn, nodes map[string]net.Conn) {
	for {
		d := data.Data{}
		receiver.UnicastReceive(c, &d)
		fmt.Println("Data received from client")
		fmt.Println("Data received is: <", d.Key, ",", d.Value, ">")
		switch d.ReadOrWrite {

		case 0: //client is reading
			fmt.Println("Client wants to Read")
			// send all the other servers that we want to read this key
			sendToOtherServers(d, nodes)

			// waits for N - f acks
			// adds the current server's key-value pair to compare
			ackArray.a = nil

			if useDisk {
				hasKey, returnedValue := utils.ReadFromFile(serverID, d.Key)
				if hasKey {
					d.Exists = 1
					ackArray.a = append(ackArray.a, returnedValue)
				} else {
					d.Exists = 0
					ackArray.a = append(ackArray.a, d)
				}
			} else {
				index := contains(storage.s, d.Key)
				fmt.Println("Index:", index)
				if index >= 0 {
					storage.s[index].Exists = 1
					ackArray.mu.Lock()
					ackArray.a = append(ackArray.a, storage.s[index])
					ackArray.mu.Unlock()
				} else {
					d.Exists = 0
					ackArray.mu.Lock()
					ackArray.a = append(ackArray.a, d)
					ackArray.mu.Unlock()
				}
				fmt.Println("Exists:", d.Exists)
			}
			fmt.Println("Before:", ackArray.a)
			for len(ackArray.a) < allowedFailures {
			}

			fmt.Println("After:", ackArray.a)

			var counter int

			for i := range ackArray.a {
				fmt.Println(i)
				if ackArray.a[i].Exists == 1 {
					counter += 1
				}
			}
			fmt.Println("Counter: ", counter)

			if counter < allowedFailures {
				//notify the client that the provided key does not exist
				d.Ack = 0
				sender.UnicastSend(c, d)
				break
			}

			// check timestamp
			d = ackArray.a[0]
			t := ackArray.a[0].Timestamp
			for i := range ackArray.a {
				newtime := ackArray.a[i].Timestamp
				if newtime.After(t) {
					// get most recent key value pair
					d = ackArray.a[i]
				}
			}
			// send it back to the client
			d.Ack = 1
			sender.UnicastSend(c, d)

		case 1: //client is writing
			fmt.Println("Client wants to Write")
			// write to this server
			if useDisk {
				utils.WriteToFile(serverID, d)
			}

			//write to disk also for calculation stuff
			index := contains(storage.s, d.Key)
			if index >= 0 {
				storage.mu.Lock()
				storage.s[index] = d
				storage.mu.Unlock()
			} else {
				d.Exists = 1
				storage.mu.Lock()
				storage.s = append(storage.s, d)
				storage.mu.Unlock()
				d.Exists = 0
			}
			fmt.Println("Storage:", storage.s)
			// send all the other servers that we want to write this key-value pair
			sendToOtherServers(d, nodes)

			fmt.Println("Data sent to other servers")

			d.Ack = 1
			d.Exists = 1
			ackArray.mu.Lock()
			ackArray.a = append(ackArray.a, d)
			ackArray.mu.Unlock()

			// waits for N - f acks
			for len(ackArray.a) < allowedFailures {
			}

			fmt.Println(ackArray.a)

			var counter int
			for i := range ackArray.a {
				fmt.Println(i)
				if ackArray.a[i].Ack == 1 {
					counter += 1
				}
			}
			fmt.Println("Counter: ", counter)

			if counter < allowedFailures {
				//notify the client that the key value pair wasn't written
				d.Ack = 0
				sender.UnicastSend(c, d)
				break
			}
			// sends ack back to the client
			d.Ack = 1
			sender.UnicastSend(c, d)
		default:
			fmt.Println("Unreachable")
			// sender.UnicastSend(c, d)
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

func listenToOtherServers(c net.Conn) { // pass through nodes data struct and send appropriate data (ACK) to the specific channel
	for {
		d := data.Data{}
		receiver.UnicastReceive(c, &d)
		fmt.Println("Data received")

		if d.Ack == 1 {
			ackArray.mu.Lock()
			ackArray.a = append(ackArray.a, d)
			ackArray.mu.Unlock()
			continue
		}

		switch d.ReadOrWrite {
		case 0: // reading
			fmt.Println("Received a read request")
			//check if you have k,v pair in storage
			//if you have it, send it back
			var fileContainsKey bool

			if useDisk {
				fileContainsKey, _ = utils.ReadFromFile(serverID, d.Key)

				if fileContainsKey {
					// d = data in this server's storage
					d.Ack = 1
					d.Exists = 1
					sender.UnicastSend(c, d)
				} else {
					d.Ack = 1
					d.Exists = 0
					sender.UnicastSend(c, d)
				}
			} else {
				index := contains(storage.s, d.Key)
				fmt.Println("Index:", index)
				if index >= 0 {
					d = storage.s[index]
					d.Ack = 1
					d.Exists = 1
					sender.UnicastSend(c, d)
				} else {
					d.Ack = 1
					d.Exists = 0
					sender.UnicastSend(c, d)
				}
			}
			fmt.Println("Response sent with Ack:", d.Ack)
			/*
				if contains(storage.s, d.Key) >= 0 {
					var fileContainsKey bool

					if useDisk {
					}

					if fileContainsKey {
						// d.ReadOrWrite = -1
						d.Ack = 1
						d.Exists = 1
						sender.UnicastSend(c, d)
					} else {
						d := data.Data{}
						d.Ack = 1
						d.Exists = 0
						// d.ReadOrWrite = -1
						sender.UnicastSend(c, d)
					}
				} else {
					d.Ack = 1
					d.Exists = 0
					sender.UnicastSend(c, d)
				}
			*/

		case 1: // writing
			fmt.Println("Received a write request")
			index := contains(storage.s, d.Key)
			if index >= 0 {
				storage.mu.Lock()
				storage.s[index] = d
				storage.mu.Unlock()
			} else {
				storage.mu.Lock()
				storage.s = append(storage.s, d)
				storage.mu.Unlock()
				if useDisk {
					utils.WriteToFile(serverID, d)
				}
				// } else {
				// 	index := contains(storage.s, d.Key)
				// 	if index >= 0 {
				// 		storage.s[index] = d
				// 	} else {
				// 		storage.s = append(storage.s, d)
				// 	}
				// }
				// fmt.Println(storage)
				// d.ReadOrWrite = -1

			}
			d.Ack = 1
			sender.UnicastSend(c, d)

			fmt.Println("Response sent with Ack:", d.Ack)

		default:
			fmt.Println("Unreachable")
		}
	}
}

func main() {
	arguments := os.Args // takes in the command argument (ID, disk flag) and identify the port number

	// checks if host address and port # are provided
	if len(arguments) == 1 {
		fmt.Println("Please provide port")
		return
	} else if len(arguments) == 2 {
		fmt.Println("Please provide disk flag")
	}

	ID, _ := strconv.Atoi(arguments[1])
	serverID = string(arguments[1])

	if strings.Contains(arguments[2], "disk") {
		useDisk = true

		// For Testing:
		// utils.WriteToFile(string(arguments[1]), data.Data{Key: "key", Value: "value", Timestamp: time.Now()})
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

	allowedFailures = c.NumServers - c.NumFailures // sets N-f
	nodes := make(map[string]net.Conn)             // tracks server ID and InetAddress

	// TODO - goroutine to always listen to the incoming dial, connect whenever

	for i := ID + 1; i < c.NumServers; i++ {
		fmt.Println("Listening for Server", i, "on Port", c.Servers[i].Port)
		receiver.ServerListen(port, nodes)
	}

	for i := 0; i < ID; i++ {
		fmt.Println("Dialing Server", i)
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
		fmt.Println("Serving client on address", c)
		fmt.Println("--------------------------------")
	}
}
