package shardkv

import (
	"bytes"
	"log"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var lastOperation map[int64]LastOpStruct
	var currentConfig shardctrler.Config
	var kvdb map[int]Shard

	if d.Decode(&lastOperation) != nil || d.Decode(&currentConfig) != nil ||
		d.Decode(&kvdb) != nil {
		log.Fatal("kvserver failed to read persist\n")
	} else {
		kv.lastOperations = lastOperation
		kv.currentConfig = currentConfig
		kv.stateMachine = kv.getMachineStore(kvdb)
	}
}

func (kv *ShardKV) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastOperations)
	e.Encode(kv.currentConfig)
	e.Encode(kv.genMachineStore())
	data := w.Bytes()
	return data
}

func (kv *ShardKV) takeSnapshot(raftIndex int) {
	snapshot := kv.MakeSnapshot()
	kv.rf.Snapshot(raftIndex, snapshot)
}

func (kv *ShardKV) needSnapshot(proportion int) bool {
	if kv.maxraftstate != -1 {
		if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
			return true
		}
	}
	return false
}

func (kv *ShardKV) genMachineStore() map[int]Shard {
	m := make(map[int]Shard)
	for gid, shard := range kv.stateMachine {
		m[gid] = *shard
	}
	return m
}

func (kv *ShardKV) getMachineStore(genMachine map[int]Shard) map[int]*Shard {
	m := make(map[int]*Shard)
	for gid, shardp := range genMachine {
		shard := shardp
		m[gid] = &shard
	}
	return m
}
