package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"

	. "github.com/wanderer69/SmallDB/server"
)

type Settings struct {
	Port int
	Path string
}

func LoadSettings(name string) (*Settings, error) {
	data, err := ioutil.ReadFile(name) // "settings.json"
	if err != nil {
		fmt.Print(err)
		return nil, err
	}
	var s Settings
	err = json.Unmarshal(data, &s)
	if err != nil {
		fmt.Println("error:", err)
		return nil, err
	}
	return &s, nil
}

func SaveSettings(s *Settings, name string) error {
	data_1, err2_ := json.MarshalIndent(&s, "", "  ")
	if err2_ != nil {
		fmt.Println("error:", err2_)
		return err2_
	}
	_ = ioutil.WriteFile(name, data_1, 0644) // "settings.json"
	return nil
}

func main() {
	var file_settings_var string
	flag.StringVar(&file_settings_var, "file_settings", "", "file_settings")
	var port_var int
	flag.IntVar(&port_var, "port", 9091, "external port")

	flag.Parse()

	var port int
	
	//fmt.Printf("len(file_settings_var) %v\r\n", len(file_settings_var))
	if len(file_settings_var) > 0 {
		s, err_ := LoadSettings(file_settings_var)
		if err_ != nil {
			fmt.Println(err_)
			s := Settings{}
			s.Port = 9091
			err := SaveSettings(&s, file_settings_var)
			if err != nil {
				fmt.Println(err)
				return
			}
			return
		}
		port = s.Port
	} else { 
		port = port_var
	}

	SmallDBServer("/smallDB/api/v1", port)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, os.Interrupt)

	<-quit
}
