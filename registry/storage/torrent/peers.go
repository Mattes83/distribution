package torrent

import (
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/opencontainers/go-digest"
)

type PeerCache interface {
	getPeers(dgst digest.Digest) ([]torrent.Peer, error)
	addPeer(dgst digest.Digest, peer torrent.Peer, ttl time.Duration) error
}

type memPeers struct {
	cache map[digest.Digest]torrent.Peers
	mutex sync.Mutex
}

func NewInMemPeerCache() PeerCache {
	return &memPeers{cache: make(map[digest.Digest]torrent.Peers), mutex: sync.Mutex{}}
}

func (p *memPeers) getPeers(dgst digest.Digest) ([]torrent.Peer, error) {
	peers, ok := p.cache[dgst]
	if !ok {
		peers = make(torrent.Peers, 10)
		p.cache[dgst] = peers
	}
	return peers, nil
}

func (p *memPeers) addPeer(dgst digest.Digest, peer torrent.Peer, ttl time.Duration) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cache[dgst] = append(p.cache[dgst], peer)
	return nil
}
