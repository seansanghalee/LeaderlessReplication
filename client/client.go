package main

import (
	"LeaderlessReplication/config"
	"LeaderlessReplication/data"
	"LeaderlessReplication/receiver"
	"LeaderlessReplication/sender"
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// printInterface
func printInterface() {
	fmt.Println("What would you like to do?")
	fmt.Println("1. Read from the server")
	fmt.Println("2. Write to the server")
	fmt.Println("3. Workload Generator")
	fmt.Println("4. Exit")
	fmt.Print("Enter a number: ")
}

func workloadGenerator(c net.Conn) {
	reading := func(key string) {
		d := data.Data{}
		d.Key = key
		d.ReadOrWrite = 0
		sender.UnicastSend(c, d)

		receiver.UnicastReceive(c, &d)
		if d.Ack == 0 {
			fmt.Println("Key:", d.Key, "not found.")
		} else {
			fmt.Println("Value:", d.Value)
		}
	}
	writing := func(key string, value string) {
		d := data.Data{}
		// populates Data struct
		d.Key = key
		d.Value = value
		d.Timestamp = time.Now()
		d.ReadOrWrite = 1

		//send struct to server
		sender.UnicastSend(c, d)

		//wait for ack
		receiver.UnicastReceive(c, &d)
		if d.Ack == 1 {
			fmt.Println("Wrote: <", d.Key, ",", d.Value, "> ")
		} else {
			fmt.Println("ACK:", d.Ack)
			fmt.Println("<", d.Key, ",", d.Value, ">  Not Written")
		}
	}

	rounds := 10
	for i := 1; i <= rounds; i++ {
		rand.Seed(time.Now().UnixNano())
		option := rand.Intn(2)

		key := strconv.Itoa(i)
		value := strconv.Itoa(i)
		switch option {
		case 0:
			fmt.Println("--------Round", i, ": Reading", "----------")
			reading(key)
		case 1:
			fmt.Println("--------Round", i, ": Writing", "----------")
			writing(key, value)
		default:
			fmt.Println("Unreachable")
		}
	}

}

func main() {

	// reads YAML file and extract information
	yaml, err := config.ReadConf("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// dials the appropriate server
	rand.Seed(time.Now().UnixNano())
	toDial := rand.Intn(yaml.NumServers - 0)
	IP := yaml.Servers[toDial].IP
	port := yaml.Servers[toDial].Port

	c, err := net.Dial("tcp", IP+":"+port)
	if err != nil {
		fmt.Println(err)
		return
	}

	// infinite loop until user exits
	for {
		printInterface()

		// scans for user input from the console
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSuffix(input, "\n")

		switch input {
		case "1": //reading
			d := data.Data{}
			fmt.Print("Enter Key: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSuffix(key, "\n")
			d.Key = key
			d.ReadOrWrite = 0
			sender.UnicastSend(c, d)
			fmt.Println("Key sent")

			receiver.UnicastReceive(c, &d)
			if d.Ack == 0 {
				fmt.Println("Key not found")
			} else {
				fmt.Println("Value:", d.Value)
			}
		case "2": //writing
			d := data.Data{}
			fmt.Print("Enter Key: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSuffix(key, "\n")
			fmt.Print("Enter Value: ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSuffix(value, "\n")

			// populates Data struct
			d.Key = key
			d.Value = value
			d.Timestamp = time.Now()
			d.ReadOrWrite = 1

			//send struct to server
			sender.UnicastSend(c, d)
			fmt.Println("Key/Value sent")

			//wait for ack
			receiver.UnicastReceive(c, &d)
			if d.Ack == 1 {
				fmt.Println("Written")
			} else {
				fmt.Println("Not Written")
			}
		case "3":
			fmt.Println("--------Starting Generator----------")
			workloadGenerator(c)
			fmt.Println("----------Generator Complete--------")
		case "4":
			os.Exit(1)
		default:
			fmt.Println("Invalid input. Try again!")
		}
	}
}
