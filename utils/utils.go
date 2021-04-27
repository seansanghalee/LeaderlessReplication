package utils

import (
	"LeaderlessReplication/data"
	"strconv"
	"time"

	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
)

func WriteToFile(server string, data data.Data) {

	f, err := os.OpenFile(server+".txt", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		//no current file
		mydata := []byte(data.Key + ":" + data.Value + ":" + data.Timestamp.String() + ":" + strconv.Itoa(data.Ack) + ":" + strconv.Itoa(data.Exists) + ":" + strconv.Itoa(data.ReadOrWrite) + "\n")

		err := ioutil.WriteFile(server+".txt", mydata, 0777)

		if err != nil {
			fmt.Println(err)
		}
	} else {
		//found existing file
		file, _ := ioutil.ReadFile(server + ".txt") // just pass the file name

		//check if key already exists
		found, _ := ReadFromFile(server, data.Key)

		if found {
			//delete current line with value associated with key
			re := regexp.MustCompile("(?m)^.*" + data.Key + ".*$[\r\n]+")
			result := re.ReplaceAllString(string(file), "")

			//delete file
			f.Truncate(0)

			//write new content without the key to file
			if _, err = f.WriteString(result); err != nil {
				panic(err)
			}
		}

		//append new value
		if _, err = f.WriteString((data.Key + ":" + data.Value + ":" + data.Timestamp.String() + ":" + strconv.Itoa(data.Ack) + ":" + strconv.Itoa(data.Exists) + ":" + strconv.Itoa(data.ReadOrWrite) + "\n")); err != nil {
			panic(err)
		}

	}
	defer f.Close()

}

func ReadFromFile(server string, key string) (keyFound bool, value data.Data) {
	input, err := ioutil.ReadFile(server + ".txt")

	if err != nil {
		log.Fatalln(err)
	}

	lines := strings.Split(string(input), "\n")

	for _, line := range lines {
		if strings.Contains(line, key+":") {
			key := strings.Split(line, ":")[0]
			value := strings.Split(line, ":")[1]
			timestamp := strings.Split(line, ":")[2]
			ack := strings.Split(line, ":")[3]
			exists := strings.Split(line, ":")[4]
			readOrWrite := strings.Split(line, ":")[5]

			//convert string timestamp to time.Time
			layout := "Mon Jan 02 2006 15:04:05 GMT-0700"
			parsedTimeStamp, _ := time.Parse(layout, timestamp)

			parsedAck, _ := StrToInt(ack)
			parsedExists, _ := StrToInt(exists)
			parsedReadOrWrite, _ := StrToInt(readOrWrite)

			dataOutput := data.Data{Key: key, Value: value, Timestamp: parsedTimeStamp, Ack: parsedAck, Exists: parsedExists, ReadOrWrite: parsedReadOrWrite}
			fmt.Println(dataOutput)
			return true, dataOutput
			// return true, data.Data{Key: key, Value: value, Timestamp: time.Now(), Ack: 1, Exists: 1, ReadOrWrite: 1}

		}
	}
	return false, data.Data{}

}

func StrToInt(str string) (int, error) {
	nonFractionalPart := strings.Split(str, ".")
	return strconv.Atoi(nonFractionalPart[0])
}

