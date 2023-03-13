package shardkv

import (
	"bytes"
	"log"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		kv.makeStateMachine()
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var lastOperation map[int64]LastOpStruct
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config
	var kvdb map[int]*Shard

	if d.Decode(&lastOperation) != nil || d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&kvdb) != nil {
		log.Fatal("kvserver failed to read persist\n")
	} else {
		kv.lastOperations = lastOperation
		kv.lastConfig = lastConfig
		kv.currentConfig = currentConfig
		kv.stateMachine = kvdb
	}
}

func (kv *ShardKV) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastOperations)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	e.Encode(kv.stateMachine)
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

func (kv *ShardKV) makeStateMachine() {
	for i := 0; i < shardctrler.NShards; i++ {
		if _, ok := kv.stateMachine[i]; !ok {
			kv.stateMachine[i] = NewShard()
		}
	}
}
