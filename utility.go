package main

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"go.etcd.io/etcd/clientv3"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)
// some code is due to NFSB,
// see https://github.com/haochenpan/NFSB/blob/master/GNF/utilities.go
type (
	OpType   string
	OpResult string
	BmType   string
	CsType   string
)

const (
	maxBlock                  = 50 // assume maxBlock > globalMaxThread
	globalMaxThread           = 50
	blockIdPrefix             = "doc:1-"
	CassDef          BmType   = "CassDef"
	EtcdRaft         BmType   = "EtcdRaft"
	ReadWrite        CsType   = "ReadWrite"
	ReadOnly         CsType   = "ReadOnly"
	W                OpType   = "write"
	R                OpType   = "read"
	S                OpResult = "success"
	F                OpResult = "failed"
	Letters                   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	cCDefaultTimeout          = 20 * time.Second
)

const (
	Keyspace   = `ycsb`
	InsertStmt = `INSERT INTO usertable (y_id, field0) VALUES (?, ?)`
	SelectStmt = `SELECT y_id, field0 FROM usertable WHERE y_id = ? LIMIT 1`
	//InsertLwtStmt = `INSERT INTO usertable (y_id, field0) VALUES (?, ?) IF NOT EXISTS`
	UpdateLwtStmt = `UPDATE usertable SET field0 = ? WHERE y_id = ? IF field0 != ?`
	DropStmt      = `DROP KEYSPACE ycsb;`

	CreateKsRf1 = `CREATE KEYSPACE ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};`
	CreateKsRf3 = `CREATE KEYSPACE ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3};`

	CreateTbNor = `CREATE TABLE ycsb.usertable ( y_id VARCHAR PRIMARY KEY, field0 VARCHAR );`
	CreateTbTag = `CREATE TABLE ycsb.usertable ( y_id VARCHAR PRIMARY KEY, field0 VARCHAR, tag varchar );`
)

//var cluster = gocql.NewCluster("localhost")
//var cluster = gocql.NewCluster("10.0.0.1")
var cluster = gocql.NewCluster("10.0.0.1", "10.0.0.2", "10.0.0.3")

const CreateKs = CreateKsRf3
const CreateTb = CreateTbTag

var etcd1 = []string{"localhost:2379"}
var etcd2 = []string{"10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"}
var etcdEps = etcd2

type latency []time.Duration

func (lat latency) Len() int {
	return len(lat)
}

func (lat latency) Less(i, j int) bool {
	return lat[i].Nanoseconds() < lat[j].Nanoseconds()
}

func (lat latency) Swap(i, j int) {
	lat[i], lat[j] = lat[j], lat[i]
}

func (lat latency) getAvgLat() float64 {
	if lat == nil || len(lat) == 0 {
		return 0
	}

	sum := int64(0)
	for _, l := range lat {
		sum += l.Nanoseconds()
	}
	avg := (float64(sum) / float64(time.Microsecond)) / float64(len(lat))
	return avg
}

func (lat latency) get95pLat() float64 {
	if lat == nil || len(lat) == 0 {
		return 0
	}

	idx := int(float64(len(lat)) * 0.95)
	val := float64(lat[idx]) / float64(time.Microsecond)
	return val
}

type Operation struct {
	BlockIdInt int
	Type       OpType
	Result     OpResult
	Start      time.Time
	End        time.Time
}

type BmStats struct {
	Timestamp string // e.g. get from time.Now().String()

	Runtime    float64
	Throughput float64

	SRead       int
	SReadAvgLat float64
	SRead95pLat float64

	SWrite       int
	SWriteAvgLat float64
	SWrite95pLat float64

	FRead       int
	FReadAvgLat float64
	FRead95pLat float64

	FWrite       int
	FWriteAvgLat float64
	FWrite95pLat float64
}

// generates a YCSB-like verbatimBenchmark report
func (bm *BmStats) String() string {

	var str string

	str += fmt.Sprintf("[OVERALL], Timestamp, %v\n", bm.Timestamp)

	str += fmt.Sprintf("[OVERALL], RunTime(sec), %.3f\n", bm.Runtime)
	str += fmt.Sprintf("[OVERALL], Throughput(ops/sec), %.3f\n", bm.Throughput)

	if bm.SRead > 0 {
		str += fmt.Sprintf("[READ], Operations, %d\n", bm.SRead)
		str += fmt.Sprintf("[READ], AverageLatency(us), %.3f\n", bm.SReadAvgLat)
		str += fmt.Sprintf("[READ], 95thPercentileLatency(us), %.3f\n", bm.SRead95pLat)
	}

	if bm.SWrite > 0 {
		str += fmt.Sprintf("[WRITE], Operations, %d\n", bm.SWrite)
		str += fmt.Sprintf("[WRITE], AverageLatency(us), %.3f\n", bm.SWriteAvgLat)
		str += fmt.Sprintf("[WRITE], 95thPercentileLatency(us), %.3f\n", bm.SWrite95pLat)
	}

	if bm.FRead > 0 {
		str += fmt.Sprintf("[READ-FAILED], Operations, %d\n", bm.FRead)
		str += fmt.Sprintf("[READ-FAILED], AverageLatency(us), %.3f\n", bm.FReadAvgLat)
		str += fmt.Sprintf("[READ-FAILED], 95thPercentileLatency(us), %.3f\n", bm.FRead95pLat)
	}

	if bm.FWrite > 0 {
		str += fmt.Sprintf("[WRITE-FAILED], Operations, %d\n", bm.FWrite)
		str += fmt.Sprintf("[WRITE-FAILED], AverageLatency(us), %.3f\n", bm.FWriteAvgLat)
		str += fmt.Sprintf("[WRITE-FAILED], 95thPercentileLatency(us), %.3f\n", bm.FWrite95pLat)
	}
	return str
}

func randString(r *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		idx := r.Int63() % int64(len(Letters))
		b[i] = Letters[idx]
	}
	return string(b)
}

func allocSessions(sessionType BmType) {
	var localWg sync.WaitGroup
	localWg.Add(globalMaxThread)
	if sessionType == CassDef {
		cluster.Keyspace = Keyspace
		cluster.Timeout = cCDefaultTimeout
		for i := 0; i < globalMaxThread; i++ {
			go func(i int) {
				session, err := cluster.CreateSession()
				if err != nil {
					log.Fatal(err)
				}
				cassPool[i] = session
				localWg.Done()
			}(i)
		}
	} else {
		for i := 0; i < globalMaxThread; i++ {
			go func(i int) {
				cli, err := clientv3.New(clientv3.Config{
					Endpoints:   etcdEps,
					DialTimeout: 20 * time.Second,
				})
				if err != nil {
					log.Fatal(err)
				}
				etcdClnt[i] = cli
				localWg.Done()
			}(i)
		}
	}
	localWg.Wait()
}

func initDatabase(sessionType BmType) {
	src := rand.NewSource(time.Now().UnixNano())
	gen := rand.New(src)
	if sessionType == CassDef {
		if err := cassPool[0].Query(DropStmt).Exec(); err != nil {
			log.Fatal("DropStmt ", err)
		}
		if err := cassPool[0].Query(CreateKs).Exec(); err != nil {
			log.Fatal("CreateKs ", err)
		}
		if err := cassPool[0].Query(CreateTb).Exec(); err != nil {
			log.Fatal("CreateTb ", err)
		}
		for i := 0; i < maxBlock; i++ {
			blockId := blockIdPrefix + strconv.Itoa(i)
			SetBlockCassOne(*cassPool[0], &Block{blockId, randString(gen, 50)})
		}
	} else {
		cli := etcdClnt[0]
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		dresp, err := cli.Delete(ctx, blockIdPrefix, clientv3.WithPrefix())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("dresp", dresp)
		for i := 0; i < maxBlock; i++ {
			blockId := blockIdPrefix + strconv.Itoa(i)
			SetBlockEtcd(*cli, &Block{blockId, randString(gen, 50)})
		}
	}
}

func deallocSessions(sessionType BmType) {
	if sessionType == CassDef {
		for i := 0; i < globalMaxThread; i++ {
			cassPool[i].Close()
		}
	} else {
		for i := 0; i < globalMaxThread; i++ {
			if err := etcdClnt[i].Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
}
