// Copyright 2013-2016 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"

	"fmt"
	. "github.com/aerospike/aerospike-client-go"
	"github.com/prometheus/common/log"
	"github.com/quipo/statsd"
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	host       string = "127.0.0.1"
	port       int    = 3000
	namespace  string = "test"
	set        string = "demo"
	statsdHost string = "localhost"
	statsdPort int    = 8125
	rps        int    = 1000
	count      int    = 1000000000
	ttl        int    = 1
	prefix     string = "aerospike."
	errorCount int64
)

func getStatsdClient() *statsd.StatsdClient {
	statsdclient := statsd.NewStatsdClient("localhost:8125", prefix)
	err := statsdclient.CreateSocket()

	if err != nil {
		log.Infof("Error while creating statsd client %s", err.Error())
		return nil
	}

	return statsdclient
}
func main() {

	var err error

	// arguments
	flag.StringVar(&host, "host", host, "Remote host")
	flag.IntVar(&port, "port", port, "Remote port")
	flag.StringVar(&namespace, "namespace", namespace, "Namespace")
	flag.StringVar(&set, "set", set, "Set name")
	flag.StringVar(&statsdHost, "statsHost", statsdHost, "Statsd host")
	flag.IntVar(&statsdPort, "statsPort", statsdPort, "Statsd port")
	flag.IntVar(&rps, "rps", rps, "Request per second")
	flag.IntVar(&ttl, "ttl", ttl, "TTL for key")

	policy := NewWritePolicy(0, 0)
	basePolicy := NewPolicy()
	// parse flags
	flag.Parse()
	frequency := 1000000 / rps
	ticker := time.NewTicker(time.Duration(frequency) * time.Microsecond)
	// args

	client, err := NewClient(host, port)

	if err != nil {
		log.Fatalf("Error creating aerospike client %s", err.Error())
	}

	var key *Key = nil
	rec := BinMap{
		"bin1": "value1",
		"bin2": "value2",
		"bin3": "value3",
	}

	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)
	log.Infof("Start experiment")
	log.Infof("Aerospike %s:%d", host, port)
	log.Infof("Request per second rate: %d", rps)
	log.Infof("TTL: %d", ttl)
	log.Infof("Stats server %s:%d", statsdHost, statsdPort)

	for i := 0; i < count; i++ {
		for range ticker.C {
			go func() {
				keyId := rnd.Int()

				if err == nil {
					key, err = NewKey(namespace, set, fmt.Sprintf("key-%d", keyId))
				}
				begin := time.Now().UnixNano()
				err = client.Put(policy, key, rec)
				endWrite := time.Now().UnixNano()
				getStatsdClient().Timing("latency.write", (endWrite-begin))
				beginRead := time.Now().UnixNano()
				_, err = client.Get(basePolicy, key)
				endRead := time.Now().UnixNano()
				getStatsdClient().Timing("latency.read", (endRead-beginRead))
				getStatsdClient().Timing("latency.total", (endRead-begin))
				getStatsdClient().Incr("bandwidth", 1)

				if err != nil {
					atomic.AddInt64(&errorCount, 1)
				}
			}()
		}
	}

	log.Infof("Experiment is finished, error count %d", errorCount)
}
