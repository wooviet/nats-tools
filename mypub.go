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
	log.Printf("Starting publish [pubs=%d, subjs=%d, msgs=%d, msgsize=%d], subject: %s\n",
		*numPubs, totalSubjNum, *numMsgs, *msgSize, subj)

	count := int64(0)
	stat := PublishStat{}
	totalMsgNum := *numMsgs * totalSubjNum
	go func() {
		lastPubNum, lastTimeout := int64(0), int64(0)
		for range time.Tick(1 * time.Second) {
			pubNum := atomic.LoadInt64(&stat.PubNum)
			timeoutMsgs := atomic.LoadInt64(&stat.TimeoutNum)
			log.Printf("Total: %d, Published: %d, Speed: %d msgs/s %.2f MB/s, " +
				"Start: %d, Closed: %d, Timeout Msgs: Total: %d, %d msgs/s",
				totalMsgNum, pubNum, pubNum-lastPubNum,
				float64(pubNum-lastPubNum)*float64(*msgSize)/1024/1024, atomic.LoadInt64(&count),
				atomic.LoadInt64(&stat.CloseNum), timeoutMsgs, timeoutMsgs-lastTimeout)
			lastPubNum = pubNum
			lastTimeout = timeoutMsgs
		}
	}()

	finishNum := totalSubjNum
	if totalSubjNum > DefaultSplitNum {
		finishNum = totalSubjNum / 10
		if totalSubjNum%10 != 0 {
			finishNum++
		}
	}
	sp := StopProcess{}
	sp.newSp(finishNum)
	start := time.Now()
	for i, tmpSubjNum := 0, totalSubjNum; i < *numPubs; i++ {
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			log.Fatalf("Can't connect: %v, already connected: %d\n", err, i)
		}
		defer nc.Close()

		for j := 0; j < *numSubjects; j++ {
			if totalSubjNum > DefaultSplitNum {
				// 超过10W个主题，则每10个主题共用一个协程
				if atomic.LoadInt64(&count)%10 == 0 {
					if tmpSubjNum-10 >= 0 {
						go runPublisher(nc, &sp, 10, *numMsgs, *msgSize,
							*interval, i**numSubjects+j+*startSub, &stat)
						tmpSubjNum -= 10
					}else{
						go runPublisher(nc, &sp, tmpSubjNum, *numMsgs, *msgSize,
							*interval, i**numSubjects+j+*startSub, &stat)
					}
				}
			}else{
				go runPublisher(nc, &sp, 1, *numMsgs, *msgSize,
					*interval, i**numSubjects+j+*startSub, &stat)
			}
			atomic.AddInt64(&count, 1)
		}
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
	TimeoutNum int64
	PubNum     int64
	CloseNum   int64
}

type StopProcess struct {
	Done chan struct{}
	Wg sync.WaitGroup
}

func (sp *StopProcess) newSp(wgNum int) *StopProcess {
	sp.Done = make(chan struct{})
	sp.Wg.Add(wgNum)
	return sp
}

func runPublisher(nc *nats.Conn, sp *StopProcess,
	numReqs, numMsgs, msgSize, interval, num int, stat *PublishStat) {
	args := flag.Args()[0]
	msg := []byte(args)
	if msgSize > 0 {
		msg = make([]byte, msgSize)
		for i := 0; i < len(args) && i < msgSize; i++ {
			msg[i] = args[i]
		}
	}
	for i := 0; i < numMsgs; i++ {
		for j := 0; j < numReqs; j++ {
			select {
			case <-sp.Done:
				sp.Wg.Done()
				atomic.AddInt64(&stat.CloseNum, 1)
				return
			default:
			}
			subj := "subject" + strconv.Itoa(num+j)
			_, err := nc.Request(subj, msg, 2*time.Second)
			if err != nil {
				if nc.LastError() != nil {}
				if err == nats.ErrTimeout {
					atomic.AddInt64(&stat.TimeoutNum, 1)
				}
			}
			/*err := nc.Publish(subj, msg)
			if err != nil {
				log.Printf("publish failed, errmsg: %v\n", err)
				continue
			}
			nc.Flush()*/
			atomic.AddInt64(&stat.PubNum, 1)
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}
	}
	sp.Wg.Done()
	atomic.AddInt64(&stat.CloseNum, 1)
}
