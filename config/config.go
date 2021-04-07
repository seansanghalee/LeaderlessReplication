package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	NumServers  int
	NumFailures int
	Servers     []Server
}

type Server struct {
	ID   int
	IP   string
	Port string
}

func ReadConf(filename string) (*Config, error) {
	buf, err := ioutil.ReadFile(filename)

	if err != nil {
		return nil, err
	}
	c := &Config{}

	err = yaml.Unmarshal(buf, c)
	if err != nil {
		return nil, fmt.Errorf("in file %q: %v", filename, err)
	}

	return c, nil
}
