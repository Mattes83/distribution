package torrent

import (
	"sync"
	"time"

	"github.com/anacrolix/torrent"
)

type PeerCache interface {
	GetPeers(infoHash torrent.InfoHash) (torrent.Peers, error)
	AddPeer(infoHash torrent.InfoHash, peer torrent.Peer, ttl time.Duration) error
}

type memPeers struct {
	cache map[torrent.InfoHash]torrent.Peers
	mutex sync.Mutex
}

func NewInMemPeerCache() PeerCache {
	return &memPeers{cache: make(map[torrent.InfoHash]torrent.Peers), mutex: sync.Mutex{}}
}

func (p *memPeers) GetPeers(infoHash torrent.InfoHash) (torrent.Peers, error) {
	peers, ok := p.cache[infoHash]
	if !ok {
		peers = make(torrent.Peers, 10)
		p.cache[infoHash] = peers
	}
	return peers, nil
}

func (p *memPeers) AddPeer(infoHash torrent.InfoHash, peer torrent.Peer, ttl time.Duration) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cache[infoHash] = append(p.cache[infoHash], peer)
	return nil
}
