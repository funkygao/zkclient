package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/funkygao/golib/stress"
	glog "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
)

var (
	mode  string
	path  = "/_zkbench_"
	loops = 2000
	zkSvr = "localhost:2181"
)

func init() {
	flag.StringVar(&mode, "m", "get", "benchmark get|set")
	flag.Parse()
	glog.Disable()
}

func main() {
	stress.Flags.Round = 5
	stress.Flags.Tick = 5
	stress.Flags.C1 = 1
	log.SetOutput(os.Stdout)

	zc := zkclient.New(zkSvr)
	zc.DiscardZkLogger()
	if err := zc.Connect(); err != nil {
		panic(err)
	}
	defer zc.Disconnect()
	if err := zc.CreatePersistent(path, nil); err != nil {
		panic(err)
	}

	go func() {
		// block until SIGINT and SIGTERM
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Interrupt)
		<-c

		zc.DeleteTree(path)
		log.Println("bye!")
		os.Exit(0)
	}()

	var bench stress.BenchFunc
	switch mode {
	case "get":
		bench = benchGet

	case "set":
		bench = benchSet

	default:
		panic("unkown mode")
	}
	stress.RunStress(bench)
}

func benchGet(seq int) {
	zc := zkclient.New(zkSvr)
	zc.DiscardZkLogger()
	if err := zc.Connect(); err != nil {
		panic(err)
	}

	for i := 0; i < loops; i++ {
		_, err := zc.Get(path)
		if err != nil {
			stress.IncCounter("fail", 1)
		} else {
			stress.IncCounter("ok", 1)
		}
	}
}

func benchSet(seq int) {
	zc := zkclient.New(zkSvr)
	zc.DiscardZkLogger()
	if err := zc.Connect(); err != nil {
		panic(err)
	}

	data := []byte(strings.Repeat("X", 100))
	for i := 0; i < loops; i++ {
		err := zc.Set(path, data)
		if err != nil {
			stress.IncCounter("fail", 1)
		} else {
			stress.IncCounter("ok", 1)
		}
	}
}
