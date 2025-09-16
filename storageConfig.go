package main

import (
	"encoding/json"
	"flag"
	"os"
	"time"

	"github.com/hashicorp/go-version"

	"github.com/imdario/mergo"

	"github.com/liip/sheriff"

	"github.com/sirupsen/logrus"
)

// Command line flag global variables
var debug bool
var configFile string
var streamsConfigFile string

//NewStreamCore do load config files
func NewStreamCore() *StorageST {
	flag.BoolVar(&debug, "debug", true, "set debug mode")
	flag.StringVar(&configFile, "config", "config.json", "server config file path (read-only)")
	flag.StringVar(&streamsConfigFile, "streams-config", "streams.json", "streams config file path (writable)")
	flag.Parse()

	var storage StorageST

	serverData, err := os.ReadFile(configFile)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "config",
			"func":   "NewStreamCore",
			"call":   "ReadFile",
			"file":   configFile,
		}).Errorln(err.Error())
		os.Exit(1)
	}
	
	var serverConfig ServerConfigST
	err = json.Unmarshal(serverData, &serverConfig)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "config",
			"func":   "NewStreamCore",
			"call":   "Unmarshal",
			"file":   configFile,
		}).Errorln(err.Error())
		os.Exit(1)
	}

	storage.Server = serverConfig.Server
	storage.ChannelDefaults = serverConfig.ChannelDefaults

	streamsData, err := os.ReadFile(streamsConfigFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.WithFields(logrus.Fields{
				"module": "config",
				"func":   "NewStreamCore",
				"file":   streamsConfigFile,
			}).Warnln("Streams config file not found, creating empty streams configuration")
			storage.Streams = make(map[string]StreamST)
		} else {
			log.WithFields(logrus.Fields{
				"module": "config",
				"func":   "NewStreamCore",
				"call":   "ReadFile",
				"file":   streamsConfigFile,
			}).Errorln(err.Error())
			os.Exit(1)
		}
	} else {
		var streamsConfig StreamsConfigST
		err = json.Unmarshal(streamsData, &streamsConfig)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "config",
				"func":   "NewStreamCore",
				"call":   "Unmarshal",
				"file":   streamsConfigFile,
			}).Errorln(err.Error())
			os.Exit(1)
		}
		storage.Streams = streamsConfig.Streams
	}

	if storage.Streams == nil {
		storage.Streams = make(map[string]StreamST)
	}
	
	debug = storage.Server.Debug

	for i, i2 := range storage.Streams {
		for i3, i4 := range i2.Channels {
			channel := storage.ChannelDefaults
			err = mergo.Merge(&channel, i4)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "config",
					"func":   "NewStreamCore",
					"call":   "Merge",
				}).Errorln(err.Error())
				os.Exit(1)
			}
			channel.clients = make(map[string]ClientST)
			channel.ack = time.Now().Add(-255 * time.Hour)
			channel.hlsSegmentBuffer = make(map[int]SegmentOld)
			channel.signals = make(chan int, 100)
			i2.Channels[i3] = channel
		}
		storage.Streams[i] = i2
	}
	
	log.WithFields(logrus.Fields{
		"module":              "config",
		"func":                "NewStreamCore",
		"server_config":       configFile,
		"streams_config":      streamsConfigFile,
		"streams_count":       len(storage.Streams),
	}).Infoln("Configuration loaded successfully")
	
	return &storage
}

//SaveConfig saves only the streams configuration
func (obj *StorageST) SaveConfig() error {
	log.WithFields(logrus.Fields{
		"module": "config",
		"func":   "SaveConfig",
		"file":   streamsConfigFile,
	}).Debugln("Saving streams configuration")
	
	v2, err := version.NewVersion("2.0.0")
	if err != nil {
		return err
	}

	streamsConfig := StreamsConfigST{
		Streams: obj.Streams,
	}
	
	data, err := sheriff.Marshal(&sheriff.Options{
		Groups:     []string{"config"},
		ApiVersion: v2,
	}, streamsConfig)
	if err != nil {
		return err
	}
	
	res, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	
	err = os.WriteFile(streamsConfigFile, res, 0644)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "config",
			"func":   "SaveConfig",
			"call":   "WriteFile",
			"file":   streamsConfigFile,
		}).Errorln(err.Error())
		return err
	}
	
	log.WithFields(logrus.Fields{
		"module":        "config",
		"func":          "SaveConfig",
		"file":          streamsConfigFile,
		"streams_count": len(obj.Streams),
	}).Infoln("Streams configuration saved successfully")
	
	return nil
}
