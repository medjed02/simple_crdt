package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type Operation struct {
	Key       string
	Value     string
	Timestamp uint64
	NodeID    string
}

type ReplicaInfo struct {
	Address              string
	RpcClient            *rpc.Client
	NeedToSendOperations []Operation
	Synchronized         bool
}

type CRDT struct {
	stateLock  sync.Mutex
	storage    map[string]string
	timestamps map[string]uint64
	operations []Operation
	replicas   map[string]*ReplicaInfo
	clock      uint64
	nodeID     string
}

func NewCRDT(nodeID string, replicas map[string]*ReplicaInfo) *CRDT {
	return &CRDT{
		storage:    make(map[string]string),
		timestamps: make(map[string]uint64),
		replicas:   replicas,
		nodeID:     nodeID,
	}
}

func (c *CRDT) replicaRpc(replicaID string, funcName string, req, rsp any) bool {
	replicaInfo := c.replicas[replicaID]
	rpcClient := replicaInfo.RpcClient
	var err error
	if rpcClient == nil {
		replicaInfo.RpcClient, err = rpc.DialHTTP("tcp", replicaInfo.Address)
		rpcClient = replicaInfo.RpcClient
	}

	if err == nil {
		err = rpcClient.Call(funcName, req, rsp)
	}

	return err == nil
}

func (c *CRDT) ApplyOperation(op Operation) {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	currentTimestamp, exists := c.timestamps[op.Key]
	if !exists || op.Timestamp > currentTimestamp || op.Timestamp == currentTimestamp && op.NodeID > c.nodeID {
		fmt.Println("Apply operation", op)
		c.storage[op.Key] = op.Value
		c.timestamps[op.Key] = op.Timestamp
		c.operations = append(c.operations, op)
	}

	if op.Timestamp > c.clock {
		c.clock = op.Timestamp
	}
}

func (c *CRDT) incrementClock() uint64 {
	c.clock++
	return c.clock
}

func (c *CRDT) Set(updates map[string]string) {
	fmt.Println("set", updates)
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	for key, value := range updates {
		timestamp := c.incrementClock()
		op := Operation{
			Key:       key,
			Value:     value,
			Timestamp: timestamp,
			NodeID:    c.nodeID,
		}

		c.stateLock.Unlock()
		c.ApplyOperation(op)
		c.replicate(op)
		c.stateLock.Lock()
	}
}

func (c *CRDT) Get(key string) (string, bool) {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	value, exists := c.storage[key]
	return value, exists
}

func (c *CRDT) replicate(op Operation) {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	for replica, replicaInfo := range c.replicas {
		if replica == c.nodeID {
			continue
		}
		if !c.replicaRpc(replica, "CRDTRPC.ApplyOperations", []Operation{op}, nil) {
			replicaInfo.NeedToSendOperations = append(replicaInfo.NeedToSendOperations, op)
		}
	}
}

func (c *CRDT) PushFailedOperations() {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	for replica, replicaInfo := range c.replicas {
		if replica == c.nodeID {
			continue
		}
		if len(replicaInfo.NeedToSendOperations) != 0 && c.replicaRpc(replica, "CRDTRPC.ApplyOperations", replicaInfo.NeedToSendOperations, nil) {
			replicaInfo.NeedToSendOperations = []Operation{}
		}
	}
}

func (c *CRDT) Synchronize() bool {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	synchronized := true
	for replica, replicaInfo := range c.replicas {
		if replica == c.nodeID || replicaInfo.Synchronized {
			continue
		}
		var remoteOps []Operation
		replicaInfo.Synchronized = c.replicaRpc(replica, "CRDTRPC.GetOperations", struct{}{}, &remoteOps)

		c.stateLock.Unlock()
		for _, op := range remoteOps {
			c.ApplyOperation(op)
		}
		c.stateLock.Lock()

		synchronized = synchronized && replicaInfo.Synchronized
	}

	return synchronized
}

func (c *CRDT) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if value, exists := c.Get(key); exists {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Key not found"))
	}
}

func (c *CRDT) handleSet(w http.ResponseWriter, r *http.Request) {
	var updates map[string]string
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c.Set(updates)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Success"))
}

type CRDTRPC struct {
	CRDT *CRDT
}

func (rpc *CRDTRPC) ApplyOperations(ops []Operation, _ *struct{}) error {
	for _, op := range ops {
		rpc.CRDT.ApplyOperation(op)
	}
	return nil
}

func (rpc *CRDTRPC) GetOperations(_ struct{}, reply *[]Operation) error {
	rpc.CRDT.stateLock.Lock()
	defer rpc.CRDT.stateLock.Unlock()
	*reply = append(*reply, rpc.CRDT.operations...)
	return nil
}

type Config struct {
	Replicas    map[string]*ReplicaInfo
	NodeID      string
	HttpAddress string
}

func getConfig() Config {
	cfg := Config{}
	cfg.Replicas = make(map[string]*ReplicaInfo)

	id := flag.String("node", "node1", "node id, string")
	http := flag.String("http", ":5030", "node http addr, string")
	replicasString := flag.String("replicas", "", "replicas info in format $id1,$ip1;$id2,$ip2, string")
	flag.Parse()

	cfg.HttpAddress = *http
	cfg.NodeID = *id

	for _, part := range strings.Split(*replicasString, ";") {
		replicaInfo := &ReplicaInfo{}

		idAddress := strings.Split(part, ",")
		replicaInfo.Address = idAddress[1]
		cfg.Replicas[idAddress[0]] = replicaInfo
	}

	return cfg
}

func main() {
	config := getConfig()
	node := NewCRDT(config.NodeID, config.Replicas)

	// rpc
	rpcServer := rpc.NewServer()
	rpcServer.Register(&CRDTRPC{CRDT: node})
	go func() {
		log.Fatal(http.ListenAndServe(config.Replicas[config.NodeID].Address, rpcServer))
	}()

	// start synchronize
	go func() {
		for {
			if node.Synchronize() {
				break
			}
			time.Sleep(time.Second)
		}
		fmt.Println("Synchronized")
	}()

	// push failed
	go func() {
		for {
			node.PushFailedOperations()
			time.Sleep(time.Second)
		}
	}()

	http.HandleFunc("/get", node.handleGet)
	http.HandleFunc("/set", node.handleSet)
	log.Fatal(http.ListenAndServe(config.HttpAddress, nil))
}
