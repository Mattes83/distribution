package torrent

import (
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

type Peer struct {
	ip string
	id string
}

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

func (p *memPeers) getPeers(dgst digest.Digest) ([]Peer, error) {
	return p[dgst]
}

func (p *memPeers) addPeer(dgst digest.Digest, peer Peer, ttl time.Duration) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cache[dgst] = append(p.cache[dgst], peer)
	return nil
}
