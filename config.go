package main

import (
	"encoding/json"
	"os"

	"github.com/urfave/cli"
)

var cfg Config

// Config holds the app's configuration
type Config struct {
	Redis struct {
		Addr string `json:"addr"`
	} `json:"redis"`

	API struct {
		HeartbeatPath string `json:"heartbeat_path"`
	} `json:"api"`

	Processor struct {
		StorageDir    string `json:"storage_dir"`
		UserAgent     string `json:"user_agent"`
		StatsInterval int    `json:"stats_interval"`
	} `json:"processor"`

	Notifier struct {
		DownloadURL   string `json:"download_url"`
		Concurrency   int    `json:"concurrency"`
		StatsInterval int    `json:"stats_interval"`
	} `json:"notifier"`

	Backends struct {
		Kafka struct {
			BootstrapServers string `json:"bootstrap.servers"`
		} `json:"kafka"`
		HTTP struct{} `json:"http"`
	} `json:"backends"`
}

func parseCliConfig(ctx *cli.Context) error {
	return parseConfig(ctx.String("config"))
}

func parseConfig(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	dec.Decode(&cfg)

	return nil
}

// StructToMap converts a Config struct to a map
func StructToMap(cfg *Config) map[string]interface{} {
	var res map[string]interface{}
	cfgBytes, _ := json.Marshal(cfg)
	json.Unmarshal(cfgBytes, &res)

	return res
}
