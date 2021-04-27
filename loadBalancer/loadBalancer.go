package main

import (
	"LeaderlessReplication/config"
	"LeaderlessReplication/utils"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-co-op/gocron"
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

	healthCheck(serverList)

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

	currIndex := lastServedIndex
	nextIndex := (lastServedIndex + 1) % len(serverList)
	server := serverList[nextIndex]
	lastServedIndex = nextIndex

	for {
		if (currIndex == nextIndex) || server.Alive {
			break
		}
		nextIndex = (lastServedIndex + 1) % len(serverList)
		server = serverList[nextIndex]
		lastServedIndex = nextIndex
	}

	if currIndex == nextIndex {
		fmt.Println("No avaliable servers")

	} else {
		return server
	}

	mutex.Unlock()
	return server //TODO: figure out how to handle return when no avaliable servers

}

func healthCheck(servers []config.Server) {
	s := gocron.NewScheduler(time.Local)
	l := log.New(os.Stdout, "", 0)

	_, err := s.Every(2).Seconds().Do(func() {
		for server := range servers {
			health := checkHealth(servers[server])

			utils.Log(l, strconv.Itoa(serverList[server].ID)+" is "+health)

			if health == "healthy" {
				serverList[server].Alive = true
			} else {
				serverList[server].Alive = false
			}

		}
		fmt.Println("-------------------------------------------")
	})

	if err != nil {
		log.Fatalln(err)
	}

	s.StartAsync()
}

func checkHealth(server config.Server) string {
	_, err := net.Dial("tcp", server.IP+":"+server.Port)
	if err != nil {
		return "unhealthy"
	}
	return "healthy"
}
