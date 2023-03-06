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
	var lastRequestId map[int64]int

	if d.Decode(&kvdb) != nil || d.Decode(&lastRequestId) != nil {
		log.Fatal("kvserver failed to read persist\n")
	} else {
		kv.kvMu.Lock()
		kv.kvMachine.Change(kvdb)
		kv.kvMu.Unlock()
		kv.lastRequestId = lastRequestId
	}
}
