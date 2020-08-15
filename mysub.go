package main

import (
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
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
	log.Printf("[#%d] Received on [%s]: '%s', count: %d",
		i, m.Subject, string(m.Data), count)
}

const (
	DefaultNumSubs      = 1
	DefaultNumSubjects  = 10
	DefaultStartSubjNum = 0
)

func main() {
	var urls = flag.String("s", nats.DefaultURL,
		"The nats server URLs (separated by comma)")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var numSubs = flag.Int("n", DefaultNumSubs, "Number of subscribers")
	var numSubjects = flag.Int("ns", DefaultNumSubjects,
		"Number of subject subscribed by each subscriber")
	var startSub = flag.Int("ss", DefaultStartSubjNum, "Start subject number")
	var showHelp = flag.Bool("h", false, "Show help message")

	// log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	// First topic name
	subj := fmt.Sprintf("subject%d", *startSub)
	totalSubjNum := *numSubs * *numSubjects
	if totalSubjNum > 1 {
		// All topic names
		subj = fmt.Sprintf("%s - subject%d", subj, totalSubjNum + *startSub - 1)
	}

	// goroutine for printing statistics
	goroutineNum := int64(0)
	stat := ReceivedStat{}
	go func() {
		lastReceived := int64(0)
		for range time.Tick(1 * time.Second){
			Received, size := atomic.LoadInt64(&stat.Received), atomic.LoadInt64(&stat.MsgSize)
			log.Printf("[%d] [%s] Received msgs: %d, Speed: %.2f MB/s",
				atomic.LoadInt64(&goroutineNum), subj, Received,
				float64(Received - lastReceived) * float64(size) / 1024 / 1024)
			lastReceived = Received
		}
	}()

	var finishwg sync.WaitGroup
	finishwg.Add(*numSubs)
	for i := 0; i < *numSubs; i++ {
		// Connect Options.
		name := fmt.Sprintf("SUB:%d", i)
		opts := []nats.Option{nats.Name(name)}
		opts = setupConnOptions(opts)
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Printf("Can't connect: %v, already connected: %d\n", err, i)
			continue
		}
		defer nc.Close()

		go runSubscriber(nc, &finishwg, i, *numSubjects, *startSub, &stat)
		atomic.AddInt64(&goroutineNum, 1)
	}
	finishwg.Wait()
	log.Printf("%d goroutine finished, subject: %s (Press enter to end)", *numSubs, subj)
	os.Stdin.Read(make([]byte, 1))
	log.Printf("Total received msgs: %d", atomic.LoadInt64(&stat.Received))
}

type ReceivedStat struct {
	Received int64
	MsgSize int64
}

func runSubscriber(nc *nats.Conn, finishwg *sync.WaitGroup,
	num, numSubjects, startSub int, stat *ReceivedStat) {
	for i := 0; i < numSubjects; i++ {
		n := i
		subj := "subject" + strconv.Itoa(num * numSubjects + i + startSub)
		var sub *nats.Subscription
		var err error
		sub, err = nc.Subscribe(subj, func(msg *nats.Msg) {
			// if stat.Received % 1000 == 0 {
			pMsgs, pBytes, _ := sub.Pending()
			if pMsgs > 100 {
				log.Printf("[#%d] Pending (%s): Msgs:%v, Bytes:%v\n", n, subj, pMsgs, pBytes)
			}

			atomic.AddInt64(&stat.Received, 1)
			atomic.StoreInt64(&stat.MsgSize, int64(len(msg.Data)))
			msg.Respond([]byte(""))
			// printMsg(msg, num, atomic.LoadInt64(&stat.Received))
		})
		if err != nil {
			log.Printf("Subscribe %s failed, errmsg: %v\n", subj, err)
		}
		sub.SetPendingLimits(-1, -1)
		// nc.Flush()
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

	opts = append(opts, nats.ErrorHandler(natsErrHandler))
	return opts
}

func natsErrHandler(nc *nats.Conn, sub *nats.Subscription, natsErr error) {
	fmt.Printf("error: %v\n", natsErr)
	if natsErr == nats.ErrSlowConsumer {
		pendingMsgs, _, err := sub.Pending()
		if err != nil {
			fmt.Printf("couldn't get pending messages: %v", err)
			return
		}
		fmt.Printf("Falling behind with %d pending messages on subject %q.\n",
			pendingMsgs, sub.Subject)
		// Log error, notify operations...
	}
	// check for other errors
}
