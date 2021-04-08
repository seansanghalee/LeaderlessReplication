package utils

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
)

func WriteToFile(server string, key string, value string) {

	f, err := os.OpenFile("0.txt", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		//no current file
		mydata := []byte(key + ":" + value + "\n")

		err := ioutil.WriteFile(server+".txt", mydata, 0777)

		if err != nil {
			fmt.Println(err)
		}
	} else {
		//found existing file
		file, _ := ioutil.ReadFile(server + ".txt") // just pass the file name

		//check if key already exists
		found, _ := ReadFromFile(server, key)

		if found {
			//delete current line with value associated with key
			//regex to find the key
			re := regexp.MustCompile("(?m)^.*" + key + ".*$[\r\n]+")
			result := re.ReplaceAllString(string(file), "")

			//delete file
			f.Truncate(0)

			//write new content without the key to file
			if _, err = f.WriteString(result + "\n"); err != nil {
				panic(err)
			}
		}

		//append new value
		if _, err = f.WriteString(key + ":" + value + "\n"); err != nil {
			panic(err)
		}

	}
	defer f.Close()

}

func ReadFromFile(server string, key string) (keyFound bool, value string) {
	input, err := ioutil.ReadFile(server + ".txt")

	if err != nil {
		log.Fatalln(err)
	}

	lines := strings.Split(string(input), "\n")

	for _, line := range lines {
		if strings.Contains(line, key) {
			value := strings.Split(line, ":")[1]
			fmt.Println("Value for key", key, "is:", value)
			return true, value
		}
	}
	return false, "null"

}
