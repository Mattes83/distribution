// Package torrent - returns bittorrent file w.r.t blob content

package torrent

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"time"

	digest "github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

type torrentBlobServer struct {
	BlobServer
	BlobProvider  *blobProvider
	peerCache     *PeerCache
	peerAvaialble time.Duration
	torrents      TorrentStore // temp storing torrents in mem
}

type TorrentStore map[digest.Digest][]byte

func NewTorrentBlobServer(blobServer *BlobServer, blobProvider *BlobProvider) *BlobServer {
	return &torrentBlobServer{
		Blobserver:    blobServer,
		blobProvider:  blobProvider,
		peerCache:     NewInMemPeerCache(),
		peerAvaialble: 4 * time.Hours(),
		torrents:      make(TorrentStore),
	}
}

func supportsBittorrent(request *http.Request) bool {
	for _, accept := range request.Header.Get("Accept") {
		for _, mediaType := range strings.Split(accept, ",") {
			if mediaType == "application/x-bittorrent" {
				return true
			}
		}
	}
	return false
}

// GetContent will return torrent file as response if client advertises its support
func (t *torrentBlobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	// Return torrent file if client supports bittorrent
	request, err := dcontext.GetRequest(ctx)
	if err != nil {
		return t.BlobServer.ServeBlob(ctx, w, r, dgst)
	}
	if !supportsBittorrent(request) {
		return t.BlobServer.ServeBlob(ctx, w, r, dgst)
	}
	// add this IP to list of peers
	t.peerCache.AddPeer(dgst, dcontext.RemoteAddr(request), t.peerAvaialble)
	// create torrent file in background if it doesn't already exist and skip for now
	torrent, ok := t.torrents[dgst]
	if !ok {
		// create torrent in background and serve the content for now
		go createTorrentFile(ctx, t.torrents, t.blobProvider, dgst)
		return t.BlobServer.ServeBlob(ctx, w, r, dgst)
	}
	w.Header().Set("Content-Type", "application/x-bittorrent")
	w.Write(torrent)
	return nil
}

func createTorrentFile(ctx context.Context, torrents TorrentStore, BlobProvider *bp, dgst digest.Digest) {
	r, err := bp.Open(ctx, dgst)
	defer r.Close()
	m, err := metaInfo.Load(r)
	if err != nil {
		logrus.Errorf("Couldn't get content of %v to generate torrent", dgst)
		return
	}
	buff := bytes.NewBuffer(nil)
	m.Write(buff)
	torrents[dgst] = m.Bytes()
}
