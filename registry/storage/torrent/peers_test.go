package torrent

import (
	"fmt"
	"net"
	"testing"

	"github.com/anacrolix/torrent"
	digest "github.com/opencontainers/go-digest"
)

func testNew(t *testing.T) {
	p := NewInMemPeerCache()
	dgst := digest.NewDigestFromHex("sha256", "6fe15d4cbc64ea9bf5ff4f9c56bb70a4fca285063e0005453915e989b96d937c")
	peers, _ := p.getPeers(dgst)
	fmt.Printf("%v", peers)
	var b [20]byte
	_ = p.addPeer(dgst, torrent.Peer{Id: b, IP: net.IPv4(1, 1, 1, 1), Port: 1234}, 0)
	peers, _ = p.getPeers(dgst)
	fmt.Printf("%v", peers)
}
