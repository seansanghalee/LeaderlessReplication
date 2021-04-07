package sender

import (
	"LeaderlessReplication/data"
	"encoding/gob"
	"fmt"
	"net"
	"strconv"
)

// Dial calls the provided network address to create a TCP connection
// func Dial(i int, ID int, IDs[]string, IPs []string, ports []string, nodes map[string]net.Conn) {
func Dial(i int, ID int, IP string, port string, nodes map[string]net.Conn) {

	c, err := net.Dial("tcp", IP+":"+port)
	if err != nil {
		fmt.Println(err)
		return
	}

	stringID := strconv.Itoa(ID)
	enc := gob.NewEncoder(c)
	enc.Encode(stringID)
	fmt.Println("Sent", ID)
	fmt.Println("Added", i)
	nodes[strconv.Itoa(i)] = c

	// fmt.Println(nodes)
}

// UnicastSend sends a message to other process via TCP channel
func UnicastSend(destination net.Conn, data data.Data) {
	enc := gob.NewEncoder(destination)
	enc.Encode(data)
}

// SendExit sends exit signals to connected nodes
func SendExit(nodes map[string]net.Conn, r int) {
	for _, value := range nodes {
		m := data.Data{}
		UnicastSend(value, m)
	}
}
