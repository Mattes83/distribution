package middleware

import (
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

type Peer string

type PeerCache interface {
	getPeers(dgst digest.Digest) ([]Peer, error)
	addPeer(dgst digest.Digest, peer Peer, ttl time.Duration) error
}

type memPeers struct {
	cache map[digest.Digest][]Peer
	mutex sync.Mutex
}

func NewInMemPeerCache() *PeerCache {
	return &memPeers{cache: make(map[digest.Digest][]Peer), mutex: sync.Mutex{}}
}

func (memPeers *p) getPeers(blob digest.Digest) ([]Peer, error) {
	return p[blob]
}

func (memPeers *p) addPeer(blob digest.Digest, peer Peer, ttl time.Duration) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cache[digest] = append(p.cache[digest], peer)
	return nil
}
