package main

import (
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func usage() {
	log.Printf("Usage: mypub [-s server] [-np num_publishers] [-nm num_msgs] [-ms msg_size] <msg>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

const (
	DefaultNumPubs     = 1
	DefaultNumSubjects = 10
	DefaultNumMsgs     = 100000
	DefaultMessageSize = 128
	DefaultMsgInterval = 0
)

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of publishers")
	var numSubjects = flag.Int("ns", DefaultNumSubjects, "Number of subject published simultaneously by each publisher")
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

	subj := "subject0"
	totalSubjNum := *numPubs * *numSubjects
	if totalSubjNum > 1 {
		subj = fmt.Sprintf("subject0 - subject%d", totalSubjNum-1)
	}
	log.Printf("Starting publish [pubs=%d, subjs=%d, msgs=%d, msgsize=%d], subject: %s\n",
		*numPubs, totalSubjNum, *numMsgs, *msgSize, subj)

	published := Published{}
	totalMsgNum := *numMsgs * totalSubjNum
	go func() {
		lastPubNum := int64(0)
		for range time.Tick(1 * time.Second) {
			pubNum := published.get()
			log.Printf("Total: %d, Published: %d, Speed: %.2f MB/s",
				totalMsgNum, pubNum, float64(pubNum - lastPubNum) * float64(*msgSize) / 1024 / 1024)
			lastPubNum = pubNum
		}
	}()

	var finishwg sync.WaitGroup
	finishwg.Add(totalSubjNum)
	start := time.Now()
	for i := 0; i < *numPubs; i++ {
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Fatalf("Can't connect: %v, already connected: %d\n", err, i)
		}
		defer nc.Close()

		for j := 0; j < *numSubjects; j++ {
			go runPublisher(nc, &finishwg, *numMsgs, *msgSize,
				*interval, i * *numSubjects+j, &published)
		}
	}
	finishwg.Wait()
	log.Printf("Total: %d, Published: %d, Used: %v",
		totalMsgNum, published.get(), time.Since(start))
}

type Published struct {
	published int64
	rwMutex sync.RWMutex
}

func (p *Published) add(n int64) {
	p.rwMutex.Lock()
	p.published += n
	p.rwMutex.Unlock()
}

func (p *Published) get() int64 {
	p.rwMutex.RLock()
	published := p.published
	p.rwMutex.RUnlock()
	return published
}

func runPublisher(nc *nats.Conn, finishwg *sync.WaitGroup,
	numMsgs, msgSize, interval, num int, published *Published) {
	args := flag.Args()[0]
	msg := []byte(args)
	if msgSize > 0 {
		msg = make([]byte, msgSize)
		for i := 0; i < len(args); i++ {
			msg[i] = args[i]
		}
	}
	subj := "subject" + strconv.Itoa(num)
	for i := 0; i < numMsgs; i++ {
		_, err := nc.Request(subj, msg, 2*time.Second)
		if err != nil {
			if nc.LastError() != nil {
				log.Fatalf("%v for request (nc.lastError)", nc.LastError())
			}
			log.Fatalf("%v for request", err)
		}
		/*err := nc.Publish(subj, msg)
		if err != nil {
			log.Printf("publish failed, err: %v\n", err)
			continue
		}*/
		published.add(1)
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
	nc.Flush()
	finishwg.Done()
}
