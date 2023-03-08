package shardctrler

import "sort"

type MemoryConfigStateMachine struct {
	configs []Config // indexed by config num
}

func NewMemoryConfigSM() *MemoryConfigStateMachine {
	stateMachine := MemoryConfigStateMachine{configs: make([]Config, 1)}
	stateMachine.configs[0].Groups = map[int][]string{}
	return &stateMachine
}

// Join: add new replica groups.
// The very first configuration should be numbered zero. It should contain no groups,
// and all shards should be assigned to GID zero (an invalid GID).
// The next configuration (created in response to a Join RPC) should be numbered 1, &c.
// There will usually be significantly more shards than groups (i.e., each group will serve
// more than one shard), in order that load can be shifted at a fairly fine granularity.
func (cf *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	lastConfig := cf.configs[len(cf.configs)-1]
	newConfig := Config{len(cf.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	s2g := Group2Shards(&newConfig)
	for {
		target, source := GetGIDWithMinimumShards(s2g), GetGIDWithMaximumShards(s2g)
		if source != 0 && len(s2g[source])-len(s2g[target]) <= 1 {
			break
		}
		s2g[target] = append(s2g[target], s2g[source][0])
		s2g[source] = s2g[source][1:]
	}

	var newShards [NShards]int
	for gid, shards := range s2g {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	newConfig.Shards = newShards
	cf.configs = append(cf.configs, newConfig)
	return OK
}

// The shardctrler should create a new configuration that does not include those groups,
// and that assigns those groups' shards to the remaining groups. The new configuration should
// divide the shards as evenly as possible among the groups, and should move as few shards as
// possible to achieve that goal.
func (cf *MemoryConfigStateMachine) Leave(gids []int) Err {
	lastConfig := cf.configs[len(cf.configs)-1]
	newConfig := Config{len(cf.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	s2g := Group2Shards(&newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		if shards, ok := s2g[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(s2g, gid)
		}
	}

	var newShards [NShards]int
	// load balancing is performed only when raft groups exist
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := GetGIDWithMinimumShards(s2g)
			s2g[target] = append(s2g[target], shard)
		}
		for gid, shards := range s2g {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	newConfig.Shards = newShards
	cf.configs = append(cf.configs, newConfig)
	return OK
}

// The shardctrler should create a new configuration in which the shard is assigned to
// the group. The purpose of Move is to allow us to test your software. A Join or Leave
// following a Move will likely un-do the Move, since Join and Leave re-balance.
func (cf *MemoryConfigStateMachine) Move(shard int, gid int) Err {
	lastConfig := cf.configs[len(cf.configs)-1]
	newConfig := Config{len(cf.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	cf.configs = append(cf.configs, newConfig)
	return OK
}

// The Query RPC's argument is a configuration number. The shardctrler replies with the
// configuration that has that number. If the number is -1 or bigger than the biggest known
// configuration number, the shardctrler should reply with the latest configuration.
// The result of Query(-1) should reflect every Join, Leave, or Move RPC that the shardctrler
// finished handling before it received the Query(-1) RPC.
func (cf *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	lastConfig := cf.configs[len(cf.configs)-1]
	if num < 0 || num > lastConfig.Num {
		num = lastConfig.Num
	}
	//TODO(infdahai): Query(-1) should reflect all operations before recieving Query(-1).
	return cf.configs[num], OK
}

func GetGIDWithMinimumShards(s2g map[int][]int) int {
	// make iteration deterministic
	// so need sort(because it occurs error when it exists multiple mimimum shards)
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	ind, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(s2g[gid]) < min {
			ind, min = gid, len(s2g[gid])
		}
	}
	return ind
}

func GetGIDWithMaximumShards(s2g map[int][]int) int {
	// always choose gid 0 if there is any
	if shards, ok := s2g[0]; ok && len(shards) > 0 {
		return 0
	}

	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// find GID with maximum shards
	ind, max := -1, -1
	for _, gid := range keys {
		if len(s2g[gid]) > max {
			ind, max = gid, len(s2g[gid])
		}
	}
	return ind
}

func Group2Shards(config *Config) map[int][]int {
	s2g := make(map[int][]int)
	for shard, gid := range config.Shards {
		s2g[gid] = append(s2g[gid], shard)
	}
	for gid := range config.Groups {
		if _, ok := s2g[gid]; !ok {
			s2g[gid] = make([]int, 0)
		}
	}
	// if shards, ok := s2g[0]; ok && len(shards) > 0 {
	// 	for gid := range config.Groups {
	// 		s2g[gid] = append(s2g[gid], s2g[0][0])
	// 		s2g[0] = s2g[0][1:]
	// 		break
	// 	}
	// }

	return s2g
}

func deepCopy(val map[int][]string) map[int][]string {
	newVal := make(map[int][]string)
	for k, v := range val {
		str := make([]string, len(v))
		copy(str, v)
		newVal[k] = v
	}
	return newVal
}
