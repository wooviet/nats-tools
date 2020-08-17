package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	// "runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

func usage() {
	log.Printf("Usage: mypub [-s server] [-np num_publishers] " +
		"[-nm num_msgs] [-ms msg_size] <msg>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

const (
	DefaultNumPubs      = 1
	DefaultNumSubjects  = 10
	DefaultStartSubjNum = 0
	DefaultNumMsgs      = 100000
	DefaultMessageSize  = 128
	DefaultMsgInterval  = 0
	DefaultSplitNum     = 100000
	DefaultSplitGoNum   = 10
	DefaultMaxInflight  = 2000
)

func main() {
	var urls = flag.String("s", nats.DefaultURL,
		"The nats server URLs (separated by comma)")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of publishers")
	var numSubjects = flag.Int("ns", DefaultNumSubjects,
		"Number of subject published by each publisher")
	var splitNum = flag.Int("sn", DefaultSplitGoNum, "Split number")
	var startSub = flag.Int("ss", DefaultStartSubjNum, "Start subject number")
	var numMsgs = flag.Int("nm", DefaultNumMsgs, "Number of Messages to Publish")
	var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message")
	var interval = flag.Int("t", DefaultMsgInterval, "Send message interval (ms)")
	var showHelp = flag.Bool("h", false, "Show help message")

	// log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 1 {
		showUsageAndExit(1)
	}

	if *numMsgs <= 0 {
		log.Fatal("Number of messages should be greater than zero.")
	}

	if *interval < 0 {
		log.Fatal("Number of messages interval should be greater than or equal to zero.")
	}

	if *splitNum <= 0 {
		log.Fatal("Split number should be greater than zero.")
	}

	// Connect Options.
	// opts := []nats.Option{nats.Name("NATS Sample Publisher"), nats.UseOldRequestStyle()}

	// First topic name
	subj := fmt.Sprintf("subject%d", *startSub)
	totalSubjNum := *numPubs * *numSubjects
	if totalSubjNum > 1 {
		// All topic names
		subj = fmt.Sprintf("%s - subject%d", subj, totalSubjNum+*startSub-1)
	}
	log.Printf("Starting publish [pubs=%d, subjs=%d, msgs=%d, msgsize=%d], subject: %s\n",
		*numPubs, totalSubjNum, *numMsgs, *msgSize, subj)

	goroutineNum := int64(0)
	stat := PublishStat{}
	totalMsgNum := *numMsgs * totalSubjNum

	// goroutine for printing statistics
	go func() {
		lastPubNum, lastTimeoutMsgs := int64(0), int64(0)
		for range time.Tick(time.Second) {
			pubNum := atomic.LoadInt64(&stat.PubNum)
			inflight := atomic.LoadInt64(&stat.InflightRequests)
			waiting := atomic.LoadInt64(&stat.Waiting)
			fast := atomic.LoadInt64(&stat.Fast)
			slow := atomic.LoadInt64(&stat.Slow)

			// Total Failed Requests
			timeoutMsgs := atomic.LoadInt64(&stat.TimeoutNum)
			log.Printf("goroutineNum: %d, Closed: %d, Total: %d, Published msgs: %d, "+
				"Speed: %d msgs/s %.2f MB/s, Timeout Msgs: Total: %d, %d msgs/s. Inflight: %v, Waiting: %v, Fast: %v, Slow: %v",
				atomic.LoadInt64(&goroutineNum),
				atomic.LoadInt64(&stat.CloseNum),
				totalMsgNum,
				pubNum,
				pubNum-lastPubNum,
				float64(pubNum-lastPubNum)*float64(*msgSize)/1024/1024,
				timeoutMsgs,
				timeoutMsgs-lastTimeoutMsgs,
				inflight,
				waiting,
				fast,
				slow,
			)
			lastPubNum = pubNum
			lastTimeoutMsgs = timeoutMsgs
		}
	}()

	// If there are more than DefaultSplitNum topics, each DefaultSplitGoNum topic shares a goroutine
	finishNum := totalSubjNum
	if totalSubjNum > DefaultSplitNum {
		finishNum = totalSubjNum / *splitNum
		if totalSubjNum%*splitNum != 0 {
			finishNum++
		}
	}
	sp := StopProcess{}
	sp.newSp(finishNum)
	start := time.Now()
	fmt.Println(*numPubs, "clients using", *numSubjects, "goroutines")
	for i, tmpSubjNum := 0, totalSubjNum; i < *numPubs; i++ {
		n := i
		name := fmt.Sprintf("PUB:%d", n)
		opts := []nats.Option{nats.Name(name)}
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Printf("Can't connect: %v, already connected: %d\n", err, i)
			continue
		}
		defer nc.Close()

		go func() {
			// 'Warm up' to setup the async request handler and give some time
			// and make extra PING/PONG roundtrip to ensure that the SUB interest
			// from the request is in the connected server.
			nc.Request("fake.warmup", []byte("WARMUP"), 1*time.Millisecond)

			// First request is slightly slower than the rest because
			// of initializing the request handler for the client,
			// so make a roundtrip to try to ensure servers are ready.
			err := nc.Flush()
			if err != nil {
				log.Fatal(err)
			}

			// Wait as well for client's _INBOX.> interest to travel
			// to the other cluster, otherwise we will get a few
			// timeouts when making the first requests.
			time.Sleep(500 * time.Millisecond)

			for j := 0; j < *numSubjects; j++ {
				pubName := fmt.Sprintf("%s:%d", name, j)
				// If there are more than DefaultSplitNum topics, each DefaultSplitGoNum topic shares a goroutine
				if totalSubjNum > DefaultSplitNum {
					if atomic.LoadInt64(&goroutineNum)%int64(*splitNum) == 0 {
						numReqs := tmpSubjNum
						if tmpSubjNum-*splitNum >= 0 {
							numReqs = *splitNum
							tmpSubjNum -= *splitNum
						}
						go runPublisher(pubName, nc, &sp, numReqs, *numMsgs, *msgSize,
							*interval, i**numSubjects+j+*startSub, &stat)
					}
				} else {
					go runPublisher(pubName, nc, &sp, 1, *numMsgs, *msgSize,
						*interval, i**numSubjects+j+*startSub, &stat)
				}
				atomic.AddInt64(&goroutineNum, 1)
			}
		}()
	}
	log.Printf("%d goroutine finished (Press enter to end)", finishNum)
	os.Stdin.Read(make([]byte, 1))
	close(sp.Done)
	sp.Wg.Wait()
	log.Printf("Total: %d, Published: %d, Closed: %d Used: %v",
		totalMsgNum, atomic.LoadInt64(&stat.PubNum),
		atomic.LoadInt64(&stat.CloseNum), time.Since(start))
}

type PublishStat struct {
	TimeoutNum       int64
	PubNum           int64
	CloseNum         int64
	InflightRequests int64
	Waiting          int64
	Fast             int64
	Slow             int64
}

type StopProcess struct {
	Done chan struct{}
	Wg   sync.WaitGroup
}

func (sp *StopProcess) newSp(wgNum int) *StopProcess {
	sp.Done = make(chan struct{})
	sp.Wg.Add(wgNum)
	return sp
}

func runPublisher(name string, nc *nats.Conn, sp *StopProcess,
	numReqs, numMsgs, msgSize, interval, num int, stat *PublishStat) {
	// Message content
	args := flag.Args()[0]
	msg := []byte(args)
	if msgSize > 0 {
		msg = make([]byte, msgSize)
		for i := 0; i < len(args) && i < msgSize; i++ {
			msg[i] = args[i]
		}
	}

	slowDeadline := 1 * time.Second
	for i := 0; i < numMsgs; i++ {
		// Make an initial server to roundtrip to try to ensure both
		// client and server are ready for the burst of requests.
		err := nc.Flush()
		if err != nil {
			log.Printf("SLOW FLUSH: %v:i:%d\t\n", name, i)
		}

		for j := 0; j < numReqs; j++ {
			select {
			case <-sp.Done:
				sp.Wg.Done()
				atomic.AddInt64(&stat.CloseNum, 1)
				return
			default:
			}

			subj := "subject" + strconv.Itoa(num+j)

			inflight := atomic.LoadInt64(&stat.InflightRequests)
			if inflight > DefaultMaxInflight {
				atomic.AddInt64(&stat.Waiting, 1)
				time.Sleep(time.Duration(4*interval) * time.Millisecond)
				atomic.AddInt64(&stat.Waiting, -1)
			}

			requestStart := time.Now()
			atomic.AddInt64(&stat.InflightRequests, 1)
			_, err := nc.Request(subj, msg, 2*time.Second)
			atomic.AddInt64(&stat.InflightRequests, -1)

			// Find slow requests that aren't the first request from a burst.
			responseTime := time.Since(requestStart)
			if responseTime > slowDeadline {
				atomic.AddInt64(&stat.Slow, 1)
				// log.Printf("SLOW: %v:i:%d:j:%d\t-\t%v\t%v\n", name, i, j, responseTime, subj)
			} else {
				atomic.AddInt64(&stat.Fast, 1)
			}

			if err != nil {
				log.Printf("ERROR: %v:i:%d:j:%d\t-\t%v\t%v\t%v\n", name, i, j, err, subj, responseTime)
				if err == nats.ErrTimeout {
					atomic.AddInt64(&stat.TimeoutNum, 1)
				}
			}
			atomic.AddInt64(&stat.PubNum, 1)
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}

		// Add wait between each burst of requests.
		atomic.AddInt64(&stat.Waiting, 1)
		time.Sleep(time.Duration(2*interval) * time.Millisecond)
		atomic.AddInt64(&stat.Waiting, -1)
	}
	sp.Wg.Done()
	atomic.AddInt64(&stat.CloseNum, 1)
}
