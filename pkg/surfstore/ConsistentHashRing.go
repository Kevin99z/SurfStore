package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap     map[string]string
	orderedHashes []string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	for _, hash := range c.orderedHashes {
		if hash > blockId {
			return c.ServerMap[hash]
		}
	}
	return c.ServerMap[c.orderedHashes[0]]
}

func Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	hashRing := make(map[string]string)
	var hashes []string
	for _, addr := range serverAddrs {
		hash := Hash("blockstore" + addr)
		hashRing[hash] = addr
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	return &ConsistentHashRing{
		ServerMap:     hashRing,
		orderedHashes: hashes,
	}
}
