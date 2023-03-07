package kvraft

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{KV: make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}

func (memoryKV *MemoryKV) Change(kvmap map[string]string) {
	memoryKV.KV = kvmap
}
