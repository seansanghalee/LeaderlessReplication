package main

import (
	"LeaderlessReplication/config"
	"fmt"
	"log"
	"net"
	"sync"
)

var (
	lastServedIndex = 0
	serverList      = []config.Server{}
	mutex           sync.Mutex
)

func main() {
	fmt.Println("Load Balancer Initialized")

	// reads YAML file and extracts information
	yaml, err := config.ReadConf("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	serverList = yaml.Servers
	port := "8081"

	// listen on all interfaces
	fmt.Println("Listening to connections on Port: ", port)
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Println("Error listening on port:", err)
	}

	for {
		// accept connection on port
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting request:", err)
		}

		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	// send back port/ip
	conn.Write([]byte(getServer().IP + ":" + getServer().Port + "\n"))
	fmt.Println("lastServedIndex:", lastServedIndex)

}

func getServer() config.Server {
	mutex.Lock()
	nextIndex := (lastServedIndex + 1) % len(serverList)
	server := serverList[nextIndex]
	lastServedIndex = nextIndex
	mutex.Unlock()
	return server
}
