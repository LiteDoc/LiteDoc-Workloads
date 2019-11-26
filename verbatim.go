package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

func producer(toProducer <-chan Operation, toConsumer chan<- Operation,
	opMax int) {
	src := rand.NewSource(time.Now().UnixNano())
	ran := rand.New(src)

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
				blockIdInt = ran.Intn(maxBlock)
				if outstanding[blockIdInt] != 1 {
					break
				}
			}
			op.BlockIdInt = blockIdInt
			a, b := ran.Int63(), int64(0.8*float64(math.MaxInt64))
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
	//fmt.Println("outstanding", outstanding)
	producerExitWg.Done()

}

func consumer(toObserver chan<- Operation, toConsumer <-chan Operation,
	txIdx int, bmType BmType) {
	src := rand.NewSource(time.Now().UnixNano() + int64(txIdx))
	ran := rand.New(src)

	for op := range toConsumer {
		blockId := blockIdPrefix + strconv.Itoa(op.BlockIdInt)
		op.Start = time.Now()
		if op.Type == R {
			//fmt.Println(txIdx, "get", blockId)
			if bmType != EtcdRaft {
				GetBlockCass(*cassPool[txIdx], blockId)
			} else {
				GetBlockEtcd(*etcdClnt[txIdx], blockId)
			}
		} else {
			b := &Block{blockId, randString(ran, 50)}
			if bmType == CassOne {
				SetBlockCassOne(*cassPool[txIdx], b)
			} else if bmType == CassLwt {
				SetBlockCassLwt(*cassPool[txIdx], b)
			} else {
				SetBlockEtcd(*etcdClnt[txIdx], b)
			}
		}
		op.End = time.Now()
		op.Result = S // TODO
		toObserver <- op
	}

	//fmt.Println(txIdx, "consumer exit")
	consumerExitWg.Done()
}

func observer(toProducer chan<- Operation, toObserver <-chan Operation,
	simple bool) {
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
		if !simple {
			toProducer <- op
		}
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
	//fmt.Println("observer exit")
	observerExitWg.Done()
}

func benchmark(currMaxThread int, bmType BmType) {
	producerExitWg.Add(1)
	observerExitWg.Add(1)
	consumerExitWg.Add(currMaxThread)

	toProducer := make(chan Operation)
	toObserver := make(chan Operation)
	toConsumer := make(chan Operation)

	go producer(toProducer, toConsumer, 5000)
	go observer(toProducer, toObserver, false)
	for i := 0; i < currMaxThread; i++ {
		go consumer(toObserver, toConsumer, i, bmType)
	}
	producerExitWg.Wait()
	consumerExitWg.Wait()
	close(toObserver)
	observerExitWg.Wait()
}
