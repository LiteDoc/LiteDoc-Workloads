package main

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"go.etcd.io/etcd/clientv3"
	"log"
	"math/rand"
	"time"
)

type Block struct {
	BlockId string
	Content string
}

func SetBlockCassOne(session gocql.Session, b *Block) {
	if err := session.Query(InsertStmt, b.BlockId, b.Content).Exec(); err != nil {
		log.Fatal("SetBlockCassOne ", err)
	}
}

func SetBlockCassLwt(session gocql.Session, b *Block) {
	if err := session.Query(UpdateLwtStmt, b.BlockId, b.Content).Exec(); err != nil {
		log.Fatal("SetBlockCassLwt ", err)
	}
}

func GetBlockCass(session gocql.Session, blockId string) Block {
	b := Block{}
	if err := session.Query(SelectStmt, blockId).Scan(&b.BlockId, &b.Content); err != nil {
		log.Fatal("GetBlockCass ", err)
	}
	return b
}

func SetBlockEtcd(cli clientv3.Client, b *Block) {
	ctx2, _ := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := cli.Put(ctx2, b.BlockId, b.Content)
	if err != nil {
		log.Fatal("SetBlockEtcd ")
	}
	//fmt.Println(resp)
}

func SetBlockEtcdTxn(cli clientv3.Client, b *Block, rand *rand.Rand) {
	ctx2, _ := context.WithTimeout(context.TODO(), 10*time.Second)
	str := randString(rand, 50)
	_, err := cli.Txn(ctx2).
		If(clientv3.Compare(clientv3.Value(b.BlockId), "<", str)).
		Then(clientv3.OpPut(b.BlockId, str)).
		Else(clientv3.OpPut(b.BlockId, b.Content)).Commit()
	if err != nil {
		log.Fatal("SetBlockEtcdTxn ")
	}
	//fmt.Println(resp)
}

func GetBlockEtcd(cli clientv3.Client, blockId string) Block {
	b := Block{}
	ctx2, _ := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := cli.Get(ctx2, blockId)
	if err != nil {
		log.Fatal("GetBlockEtcd ")
	}
	b.BlockId = blockId
	b.Content = fmt.Sprint(resp)
	return b
}
