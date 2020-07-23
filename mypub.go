package main

import (
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
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
)

func main() {
	var urls = flag.String("s", nats.DefaultURL,
		"The nats server URLs (separated by comma)")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of publishers")
	var numSubjects = flag.Int("ns", DefaultNumSubjects,
		"Number of subject published simultaneously by each publisher")
	var startSub = flag.Int("ss", DefaultStartSubjNum, "Start subject number")
	var numMsgs = flag.Int("nm", DefaultNumMsgs, "Number of Messages to Publish")
	var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message.")
	var interval = flag.Int("t", DefaultMsgInterval, "Message interval (ms)")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
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

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Publisher")}

	subj := fmt.Sprintf("subject%d", *startSub)
	totalSubjNum := *numPubs * *numSubjects
	if totalSubjNum > 1 {
		subj = fmt.Sprintf("%s - subject%d", subj, totalSubjNum+*startSub-1)
	}
	log.Printf("Starting publish [pubs=%d, subjs=%d, msgs=%d, msgsize=%d], "+
		"subject: %s (Press enter to end)\n",
		*numPubs, totalSubjNum, *numMsgs, *msgSize, subj)

	count := int64(0)
	stat := PublishStat{}
	totalMsgNum := *numMsgs * totalSubjNum
	go func() {
		lastPubNum, lastTimeout := int64(0), int64(0)
		for range time.Tick(1 * time.Second) {
			pubNum := atomic.LoadInt64(&stat.PubNum)
			timeoutMsgs := atomic.LoadInt64(&stat.TimeoutNum)
			log.Printf("[%d]Total: %d, Published: %d, Speed: %d Msgs/s %.2f MB/s, "+
				"Timeout Msgs: Total: %d, %d msgs/s",
				atomic.LoadInt64(&count), totalMsgNum, pubNum, pubNum-lastPubNum,
				float64(pubNum-lastPubNum)*float64(*msgSize)/1024/1024,
				timeoutMsgs, timeoutMsgs-lastTimeout)
			lastPubNum = pubNum
			lastTimeout = timeoutMsgs
		}
	}()

	start := time.Now()
	for i := 0; i < *numPubs; i++ {
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Fatalf("Can't connect: %v, already connected: %d\n", err, i)
		}
		defer nc.Close()

		for j := 0; j < *numSubjects; j++ {
			atomic.AddInt64(&count, 1)
			if atomic.LoadInt64(&count)%10 == 0 {
				go runPublisher(nc, *numMsgs, *msgSize, *interval,
					i**numSubjects+j, *startSub, &stat)
			}
		}
	}

	os.Stdin.Read(make([]byte, 1))
	log.Printf("Total: %d, Published: %d, Used: %v",
		totalMsgNum, atomic.LoadInt64(&stat.PubNum), time.Since(start))
}

type PublishStat struct {
	TimeoutNum int64
	PubNum  int64
}

func runPublisher(nc *nats.Conn, numMsgs, msgSize, interval, num, startSub int, stat *PublishStat) {
	args := flag.Args()[0]
	msg := []byte(args)
	if msgSize > 0 {
		msg = make([]byte, msgSize)
		for i := 0; i < len(args); i++ {
			msg[i] = args[i]
		}
	}
	for i := 0; i < numMsgs; i++ {
		for j := 0; j < 10; j++ {
			subj := "subject" + strconv.Itoa(num+j+startSub)
			_, err := nc.Request(subj, msg, 2*time.Second)
			if err != nil {
				if nc.LastError() != nil {
					// log.Fatalf("%v for request (nc.lastError)", nc.LastError())
				}
				// log.Fatalf("%v for request", err)
				if err == nats.ErrTimeout {
					atomic.AddInt64(&stat.TimeoutNum, 1)
				}
			}
			/*err := nc.Publish(subj, msg)
			if err != nil {
				log.Printf("publish failed, errmsg: %v\n", err)
				continue
			}*/
			atomic.AddInt64(&stat.PubNum, 1)
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}
	}
	nc.Flush()
}
