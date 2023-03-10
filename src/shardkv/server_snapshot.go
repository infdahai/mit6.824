package shardkv

import (
	"bytes"
	"log"

	"6.824/labgob"
)

func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvdb map[string]string
	var lastOperation map[int64]LastOpStruct

	if d.Decode(&kvdb) != nil || d.Decode(&lastOperation) != nil {
		log.Fatal("kvserver failed to read persist\n")
	} else {

		kv.lastOperations = lastOperation
	}
}

func (kv *ShardKV) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.lastOperations)
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
