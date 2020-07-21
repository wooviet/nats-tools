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
	log.Printf("Usage: mysub [-s server] [-t] [-n num_subscribers]\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func printMsg(m *nats.Msg, i int, count int64) {
	log.Printf("[#%d] Received on [%s]: '%s', count: %d", i, m.Subject, string(m.Data), count)
}

const (
	DefaultNumSubs      = 1
	DefaultNumSubjects  = 10
	DefaultStartSubjNum = 0
)

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var numSubs = flag.Int("n", DefaultNumSubs, "Number of subscribers")
	var numSubjects = flag.Int("ns", DefaultNumSubjects, "Number of subject subscribed simultaneously by each subscriber")
	var startSub = flag.Int("ss", DefaultStartSubjNum, "start subject number")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Subscriber")}
	opts = setupConnOptions(opts)

	var finishwg sync.WaitGroup
	finishwg.Add(*numSubs)

	received := Received{}
	for i := 0; i < *numSubs; i++ {
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Fatalf("Can't connect: %v, already connected: %d\n", err, i)
		}
		defer nc.Close()

		go runSubscriber(nc, &finishwg, i, *numSubjects, *startSub, &received)
	}
	finishwg.Wait()

	end := make(chan struct{})
	go func() {
		os.Stdin.Read(make([]byte, 1))
		end <- struct{}{}
	}()
	subj := fmt.Sprintf("subject%d", *startSub)
	totalSubjNum := *numSubs * *numSubjects
	if totalSubjNum > 1 {
		subj = fmt.Sprintf("%s - subject%d", subj, totalSubjNum + *startSub - 1)
	}
	log.Printf("%d subjects finished, subject: %s (Press enter to end)", totalSubjNum, subj)

	go func() {
		lastSubNum := int64(0)
		for range time.Tick(1 * time.Second){
			subNum, size := received.get()
			log.Printf("[%s] Received: %d, Speed: %.2f MB/s",
				subj, subNum, float64(subNum - lastSubNum) * float64(size) / 1024 / 1024)
			lastSubNum = subNum
		}
	}()
	<-end
}

type Received struct {
	received int64
	msgSize int
	rwMutex sync.RWMutex
}

func (r *Received) add(n int64, msg *nats.Msg) {
	r.rwMutex.Lock()
	r.received += n
	r.msgSize = len(msg.Data)
	r.rwMutex.Unlock()
}

func (r *Received) get() (int64, int) {
	r.rwMutex.RLock()
	received := r.received
	msgSize := r.msgSize
	r.rwMutex.RUnlock()
	return received, msgSize
}

func runSubscriber(nc *nats.Conn, finishwg *sync.WaitGroup,
	num, numSubjects, startSub int, received *Received) {
	for i := 0; i < numSubjects; i++ {
		subj := "subject" + strconv.Itoa(num * numSubjects + i + startSub)
		sub, err := nc.Subscribe(subj, func(msg *nats.Msg) {
			received.add(1, msg)
			msg.Respond([]byte(""))
			// printMsg(msg, num, *received)
		})
		if err != nil {
			log.Printf("Subscribe %s failed, err: %v\n", subj, err)
		}
		sub.SetPendingLimits(-1, -1)
		nc.Flush()
	}
	finishwg.Done()
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log.Printf("Disconnected: will attempt reconnects for %.0fm", totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}
