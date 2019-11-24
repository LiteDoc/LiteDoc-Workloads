package main

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"go.etcd.io/etcd/clientv3"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

var currMaxThread = 5

var cassPool [globalMaxThread]*gocql.Session
var etcdCntx [globalMaxThread]context.Context
var etcdClnt [globalMaxThread]*clientv3.Client

var exitedWg sync.WaitGroup

func producer(toProducer <-chan Operation, toConsumer chan<- Operation, toObserver chan<- Operation,
	opMax int) {
	source := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(source)

	var outstanding [maxBlock]int
	var outstandingCnt = opMax
	for i := 0; i < opMax || outstandingCnt != 0; {

		if i == opMax {
			op := <-toProducer
			outstanding[op.BlockIdInt] = 0
			outstandingCnt--
		} else {

			op := Operation{}

			var blockIdInt int
			for {
				blockIdInt = generator.Intn(maxBlock)
				if outstanding[blockIdInt] != 1 {
					break
				}
			}
			op.BlockIdInt = blockIdInt

			a, b := generator.Int63(), int64(0.8*float64(math.MaxInt64))
			if a <= b {
				op.Type = R
			} else {
				op.Type = W
			}

			select {
			case op := <-toProducer:
				outstanding[op.BlockIdInt] = 0
				outstandingCnt--
			case toConsumer <- op:
				outstanding[blockIdInt] = 1
				i++
			}
		}
	}
	close(toConsumer)
	fmt.Println("outstanding", outstanding)
	close(toObserver)
	exitedWg.Done()
}

func consumer(toObserver chan<- Operation, toConsumer <-chan Operation,
	txIdx int, txType BmType) {
	source := rand.NewSource(time.Now().UnixNano() + int64(txIdx))
	generator := rand.New(source)

	for op := range toConsumer {
		blockId := blockIdPrefix + strconv.Itoa(op.BlockIdInt)
		op.Start = time.Now()
		if op.Type == R {
			//fmt.Println(txIdx, "get", blockId)
			if txType != EtcdRaftType {
				GetBlockCass(*cassPool[txIdx], blockId)
			} else {
				GetBlockEtcd(*etcdClnt[txIdx], etcdCntx[txIdx], blockId)
			}
		} else {
			b := &Block{blockId, randString(generator, 50)}
			if txType == CassOneType {
				SetBlockCassOne(*cassPool[txIdx], b)
			} else if txType == CassLwtType {
				SetBlockCassLwt(*cassPool[txIdx], b)
			} else {
				SetBlockEtcd(*etcdClnt[txIdx], etcdCntx[txIdx], b)
			}
		}
		op.End = time.Now()
		op.Result = S // TODO
		toObserver <- op
	}

	fmt.Println(txIdx, "consumer exit")
	exitedWg.Done()
}

func observer(toProducer chan<- Operation, toObserver <-chan Operation,
) {
	//successful read, successful write, failed read, failed write
	var sRead, sWrite, fRead, fWrite int
	//latencies of those operations
	var sReadLat, sWriteLat = make(latency, 0), make(latency, 0)
	var fReadLat, fWriteLat = make(latency, 0), make(latency, 0)
	bmStart, bmEnd := time.Now(), time.Now()

	for op := range toObserver {
		if sRead == 0 && sWrite == 0 && fRead == 0 && fWrite == 0 {
			bmStart = op.Start
		}
		bmEnd = op.End
		duration := op.End.Sub(op.Start)
		if op.Result == S && op.Type == R {
			sRead++
			sReadLat = append(sReadLat, duration)
		} else if op.Result == S && op.Type == W {
			sWrite++
			sWriteLat = append(sWriteLat, duration)
		} else if op.Result == F && op.Type == R {
			fRead++
			fReadLat = append(fReadLat, duration)
		} else {
			fWrite++
			fWriteLat = append(fWriteLat, duration)
		}
		toProducer <- op
	}

	//sort 4 arrays to get percentile information
	sort.Sort(sReadLat)
	sort.Sort(sWriteLat)
	sort.Sort(fReadLat)
	sort.Sort(fWriteLat)
	//
	runTime := bmEnd.Sub(bmStart).Seconds()
	bmStats := BmStats{
		Timestamp:  time.Now().String(),
		Runtime:    runTime,
		Throughput: float64(sRead+sWrite+fRead+fWrite) / runTime,

		SRead:       sRead,
		SReadAvgLat: sReadLat.getAvgLat(),
		SRead95pLat: sReadLat.get95pLat(),

		SWrite:       sWrite,
		SWriteAvgLat: sWriteLat.getAvgLat(),
		SWrite95pLat: sWriteLat.get95pLat(),

		FRead:       fRead,
		FReadAvgLat: fReadLat.getAvgLat(),
		FRead95pLat: fReadLat.get95pLat(),

		FWrite:       fWrite,
		FWriteAvgLat: fWriteLat.getAvgLat(),
		FWrite95pLat: fWriteLat.get95pLat(),
	}
	fmt.Println(bmStats.String())
	fmt.Println("observer exit")
	exitedWg.Done()
}

func main() {

	allocSessions(EtcdRaftType)
	initDatabase(EtcdRaftType)
	//cli := etcdClnt[0]
	//ctx := etcdCntx[0]
	//SetBlockEtcd(*cli, ctx, &Block{"aaa", "bbb"})
	//GetBlockEtcd(*cli, ctx, "aaa")
	exitedWg.Add(currMaxThread + 2)
	toConsumer := make(chan Operation)
	toProducer := make(chan Operation)
	toObserver := make(chan Operation)
	go producer(toProducer, toConsumer, toObserver, 5000)
	go observer(toProducer, toObserver)
	for i := 0; i < currMaxThread; i++ {
		go consumer(toObserver, toConsumer, i, EtcdRaftType)
	}
	exitedWg.Wait()
	deallocSessions(EtcdRaftType)

}
