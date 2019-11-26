package main

import (
	"github.com/gocql/gocql"
	"go.etcd.io/etcd/clientv3"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var cassPool [globalMaxThread]*gocql.Session
var etcdClnt [globalMaxThread]*clientv3.Client

var producerExitWg sync.WaitGroup
var consumerExitWg sync.WaitGroup
var observerExitWg sync.WaitGroup

func simpleConsumer(toObserver chan<- Operation,
	txIdx int, bmType BmType, csType CsType, csArgs ...int) {
	src := rand.NewSource(time.Now().UnixNano() + int64(txIdx))
	gen := rand.New(src)
	var blockId string
	var blockIdInt int
	var numOp int

	for i, num := range csArgs {
		if i == 0 {
			blockId = blockIdPrefix + strconv.Itoa(num)
			blockIdInt = num
		} else if i == 1 {
			numOp = num
		}
	}

	for i := 0; i < numOp; {
		op := Operation{}
		op.BlockIdInt = blockIdInt
		op.Type = R
		op.Start = time.Now()
		if bmType == CassOne || bmType == CassLwt {
			GetBlockCass(*cassPool[txIdx], blockId)
		} else {
			GetBlockEtcd(*etcdClnt[txIdx], blockId)
		}
		op.End = time.Now()
		op.Result = S
		toObserver <- op
		i++

		if csType == ReadWrite {
			op := Operation{}
			op.BlockIdInt = blockIdInt
			op.Type = W
			op.Start = time.Now()
			b := &Block{blockId, randString(gen, 50)}
			if bmType == CassOne {
				SetBlockCassOne(*cassPool[txIdx], b)
			} else if bmType == CassLwt {
				SetBlockCassLwt(*cassPool[txIdx], b)
			} else if bmType == EtcdRaft {
				SetBlockEtcd(*etcdClnt[txIdx], b)
			} else if bmType == EtcdRaftTxn {
				SetBlockEtcdTxn(*etcdClnt[txIdx], b, gen)
			}
			op.End = time.Now()
			op.Result = S
			toObserver <- op
			i++
		}
	}
	consumerExitWg.Done()
}

func simpleBenchmark(readWrite int, readOnly int, bmType BmType) {
	numOpPerThread := 1000

	observerExitWg.Add(1)
	consumerExitWg.Add(readWrite + readOnly)

	toProducer := make(chan Operation)
	toObserver := make(chan Operation)

	go observer(toProducer, toObserver, true)
	for i := 0; i < readWrite; i++ {
		go simpleConsumer(toObserver, i, bmType, ReadWrite, i, numOpPerThread)
	}
	for i := 0; i < readOnly; i++ {
		go simpleConsumer(toObserver, i, bmType, ReadOnly, readWrite+i, numOpPerThread)
	}

	consumerExitWg.Wait()
	close(toObserver)
	observerExitWg.Wait()
}

func main() {
	bmType := EtcdRaft
	allocSessions(bmType)
	initDatabase(bmType)
	//benchmark(3, bmType)
	//benchmark(6, bmType)
	//benchmark(9, bmType)
	simpleBenchmark(3, 5, bmType)
	simpleBenchmark(4, 5, bmType)
	simpleBenchmark(1, 0, bmType)
	deallocSessions(bmType)
}
