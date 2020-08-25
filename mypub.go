package main

import (
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"math/rand"
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
	DefaultSplitGoNum   = 10
	DefaultReconnectTime = 5
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
	var reconnTime = flag.Int("rt", DefaultReconnectTime, "Reconnect interval (min)")
	var interval = flag.Int("t", DefaultMsgInterval, "Send message interval (ms)")
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

	if *splitNum <= 0 {
		log.Fatal("Split number should be greater than zero.")
	}

	if *reconnTime < 1 {
		log.Fatal("Reconnect interval should be greater than or equal to one.")
	}

	publishArgs := PublishArgs{urls: *urls, numPubs: *numPubs, numSubjects: *numSubjects,
		splitNum: *splitNum, startSub: *startSub, numMsgs: *numMsgs, msgSize: *msgSize,
		reconnTime: *reconnTime * 60, interval: *interval}

	// Connect Options.
	// opts := []nats.Option{nats.Name("NATS Sample Publisher"), nats.UseOldRequestStyle()}
	opts := []nats.Option{nats.Name("NATS Sample Publisher")}

	// First topic name
	subj := fmt.Sprintf("subject%d", *startSub)
	totalSubjNum := *numPubs * *numSubjects
	if totalSubjNum > 1 {
		// All topic names
		subj = fmt.Sprintf("%s - subject%d", subj, totalSubjNum+*startSub-1)
	}
	log.Printf("Starting publish [pubs=%d, subjs=%d, msgs=%d, msgsize=%d], subject: %s\n",
		*numPubs, totalSubjNum, *numMsgs, *msgSize, subj)

	stat := PublishStat{}
	totalMsgNum := *numMsgs * totalSubjNum
	// goroutine for printing statistics
	go func() {
		lastPubNum, lastTimeoutMsgs := int64(0), int64(0)
		for range time.Tick(time.Second) {
			pubNum := atomic.LoadInt64(&stat.PubNum)
			timeoutMsgs := atomic.LoadInt64(&stat.TimeoutNum)
			log.Printf("GoTotal: %d, Mgr: %d, Exit: %d, Online: %d, Closed: %d, " +
				"ReconnectSuc: %d, Failed: %d, MsgTotal: %d, Published: %d, " +
				"Speed: %d msgs/s %.2f MB/s, Timeout: Total: %d, %d msgs/s",
				atomic.LoadInt64(&stat.GoroutineNum),
				atomic.LoadInt64(&stat.goMgrNum),
				atomic.LoadInt64(&stat.goExitNum),
				atomic.LoadInt64(&stat.onlineNum),
				atomic.LoadInt64(&stat.CloseNum),
				atomic.LoadInt64(&stat.reconnectNum),
				atomic.LoadInt64(&stat.ConnFailNum),
				totalMsgNum,
				pubNum,
				pubNum-lastPubNum,
				float64(pubNum-lastPubNum)*float64(*msgSize)/1024/1024,
				timeoutMsgs,
				timeoutMsgs-lastTimeoutMsgs)
			lastPubNum = pubNum
			lastTimeoutMsgs = timeoutMsgs
		}
	}()

	// If there are more than DefaultSplitNum topics,
	// each DefaultSplitGoNum topic shares a goroutine
	finishNum := totalSubjNum
	if totalSubjNum > DefaultSplitNum {
		finishNum = totalSubjNum / *splitNum
		if totalSubjNum % *splitNum != 0 {
			finishNum++
		}
	}
	sp := StopProcess{}
	sp.newSp(finishNum)
	loopNum := int64(0)
	start := time.Now()
	for i, tmpSubjNum := 0, totalSubjNum; i < *numPubs; i++ {
		nc, err := nats.Connect(*urls, opts...)
		if err != nil {
			atomic.AddInt64(&stat.ConnFailNum, 1)
			log.Printf("Can't connect: %v, already connected: %d\n", err, i)
			continue
		}
		atomic.AddInt64(&stat.onlineNum, 1)
		defer nc.Close()

		nc.Request("fake.warmup", []byte(""), 1*time.Millisecond)
		err = nc.Flush()
		if err != nil {
			log.Fatal(err)
		}
		// time.Sleep(500 * time.Millisecond)

		for j := 0; j < *numSubjects; j++ {
			// If there are more than DefaultSplitNum topics,
			// each DefaultSplitGoNum topic shares a goroutine
			if totalSubjNum > DefaultSplitNum {
				if loopNum % int64(*splitNum) == 0 {
					numReqs := tmpSubjNum
					if tmpSubjNum - *splitNum >= 0 {
						numReqs = *splitNum
						tmpSubjNum -= *splitNum
					}
					go runPublisher(nc, numReqs, i * *numSubjects + j + *startSub,
						&publishArgs, &stat, &sp, opts...)
				}
			}else{
				go runPublisher(nc, 1, i * *numSubjects + j + *startSub,
					&publishArgs, &stat, &sp, opts...)
			}
			loopNum++
		}
	}
	log.Printf("%d goroutine finished (Press enter to end)", finishNum)
	os.Stdin.Read(make([]byte, 1))
	close(sp.Done)
	sp.Wg.Wait()
	log.Printf("GoExit: %d, Total: %d, Published: %d, Timeout: %d, Used: %v",
		atomic.LoadInt64(&stat.goExitNum), totalMsgNum, atomic.LoadInt64(&stat.PubNum),
		atomic.LoadInt64(&stat.TimeoutNum), time.Since(start))
}

type PublishStat struct {
	PubNum       int64
	TimeoutNum   int64

	GoroutineNum int64
	goMgrNum     int64
	goExitNum    int64
	onlineNum    int64
	CloseNum     int64
	reconnectNum int64
	ConnFailNum  int64
}

type PublishArgs struct {
	urls        string
	numPubs     int
	numSubjects int
	splitNum    int
	startSub    int
	numMsgs     int
	msgSize     int
	reconnTime  int
	interval    int
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

func runPublisher(nc *nats.Conn, numReqs, num int, pArgs *PublishArgs,
	stat *PublishStat, sp *StopProcess, opts ...nats.Option) {
	start := time.Now()
	atomic.AddInt64(&stat.GoroutineNum, 1)

	isMgr := num % pArgs.numSubjects == 0
	if isMgr {
		atomic.AddInt64(&stat.goMgrNum, 1)
	}

	// Message content
	args := flag.Args()[0]
	msg := []byte(args)
	if pArgs.msgSize > 0 {
		msg = make([]byte, pArgs.msgSize)
		for i := 0; i < len(args) && i < pArgs.msgSize; i++ {
			msg[i] = args[i]
		}
	}

	first := true
	rand.Seed(time.Now().Unix())
	// 保留20s用于启动
	waitTime := rand.Intn(pArgs.reconnTime-20)+20
	for i := 0; i < pArgs.numMsgs; i++ {
		for j := 0; j < numReqs; j++ {
			if isMgr && first && time.Since(start).Seconds() >= float64(waitTime) {
				first = false
				nc.Close()
				atomic.AddInt64(&stat.onlineNum, -1)
				atomic.AddInt64(&stat.CloseNum, 1)
				var err error
				nc, err = nats.Connect(pArgs.urls, opts...)
				if err != nil {
					atomic.AddInt64(&stat.ConnFailNum, 1)
					atomic.AddInt64(&stat.goExitNum, 1)
					// log.Printf("Can't connect: %v\n", err)
					sp.Wg.Done()
					return
				}
				// 这里不需要再写defer nc.close(), 因为主goroutine中会关闭
				atomic.AddInt64(&stat.reconnectNum, 1)
			}
			select {
			case <-sp.Done:
				sp.Wg.Done()
				atomic.AddInt64(&stat.goExitNum, 1)
				return
			default:
				if !isMgr && nc.IsClosed() {
					const MaxCheckCount = 10
					count := 0
					for range time.Tick(200 * time.Millisecond) {
						if count >= MaxCheckCount {
							sp.Wg.Done()
							atomic.AddInt64(&stat.goExitNum, 1)
							return
						}
						if nc.IsConnected(){
							break
						}
						count++
					}
				}
			}
			subj := "subject" + strconv.Itoa(num+j)
			_, err := nc.Request(subj, msg, 2*time.Second)
			if err != nil {
				if nc.LastError() != nil {}
				if err == nats.ErrTimeout {
					atomic.AddInt64(&stat.TimeoutNum, 1)
				}
			}
			atomic.AddInt64(&stat.PubNum, 1)
			time.Sleep(time.Duration(pArgs.interval) * time.Millisecond)
		}
	}
	sp.Wg.Done()
	atomic.AddInt64(&stat.goExitNum, 1)
}
