package kvraft

import (
	"bytes"
	"log"

	"6.824/labgob"
)

func (kv *KVServer) ReadSnapshot(snapshot []byte) {
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
		kv.kvMachine.Change(kvdb)
		kv.lastOperations = lastOperation
	}
}

func (kv *KVServer) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMachine)
	e.Encode(kv.lastOperations)
	data := w.Bytes()
	return data
}

func (kv *KVServer) takeSnapshot(raftIndex int) {
	snapshot := kv.MakeSnapshot()
	kv.rf.Snapshot(raftIndex, snapshot)
}

func (kv *KVServer) needSnapshot(proportion int) bool {
	if kv.maxraftstate != -1 {
		if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
			return true
		}
	}
	return false
}
