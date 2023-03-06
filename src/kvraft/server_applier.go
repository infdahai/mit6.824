package kvraft

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {

		} else if msg.SnapshotValid {

		}
	}
}
