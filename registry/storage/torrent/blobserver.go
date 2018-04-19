// Package torrent - returns bittorrent file w.r.t blob content

package torrent

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	digest "github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

type TorrentStore map[digest.Digest][]byte

var (
	peerCache = NewInMemPeerCache()
	peerttl   = 4 * time.Hour
	torrents  = make(TorrentStore)
)

// repository provides distribution.Repository impl with only Blobs function changed
type repository struct {
	distribution.Repository
	host url.URL
}

func NewTorrentRepository(r distribution.Repository, host url.URL) distribution.Repository {
	return repository{Repository: r, host: host}
}

type torrentBlobStore struct {
	distribution.BlobStore
	host url.URL
}

func (r repository) Blobs(ctx context.Context) distribution.BlobStore {
	return torrentBlobStore{
		BlobStore: r.Repository.Blobs(ctx),
		host:      r.host,
	}
}

//type torrentBlobServer struct {
//	distribution.BlobServer
//	BlobProvider  *distribution.BlobProvider
//	peerCache     *PeerCache
//	peerAvaialble time.Duration
//	torrents      TorrentStore // temp storing torrents in mem
//}
//
//func NewTorrentBlobServer(blobServer *distribution.BlobServer, blobProvider *distribution.BlobProvider) *distribution.BlobServer {
//	return &torrentBlobServer{
//		Blobserver:    blobServer,
//		blobProvider:  blobProvider,
//		peerCache:     NewInMemPeerCache(),
//		peerAvaialble: 4 * time.Hours(),
//		torrents:      make(TorrentStore),
//	}
//}

func supportsBittorrent(request *http.Request) bool {
	for _, accept := range request.Header["Accept"] {
		for _, mediaType := range strings.Split(accept, ",") {
			if mediaType == "application/x-bittorrent" {
				return true
			}
		}
	}
	return false
}

// ServeBlob will return torrent file as response if client advertises its support
func (t torrentBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	// Return torrent file if client supports bittorrent
	request, err := dcontext.GetRequest(ctx)
	if err != nil {
		return t.BlobStore.ServeBlob(ctx, w, r, dgst)
	}
	if !supportsBittorrent(request) {
		return t.BlobStore.ServeBlob(ctx, w, r, dgst)
	}
	// create torrent file in background if it doesn't already exist and skip for now
	torrent, ok := torrents[dgst]
	if !ok {
		// create torrent in background and serve the content for now
		go createTorrentFile(ctx, torrents, t.BlobStore, dgst)
		return t.BlobStore.ServeBlob(ctx, w, r, dgst)
	}
	w.Header().Set("Content-Type", "application/x-bittorrent")
	w.Write(torrent)
	return nil
}

func createTorrentFile(ctx context.Context, torrents TorrentStore, bs distribution.BlobStore, dgst digest.Digest) {
	// TODO: check if torrent generation is already progress before continuing
	r, err := bs.Open(ctx, dgst)
	if err != nil {
		logrus.WithError(err).Errorf("Couldn't get content of %v to generate torrent", dgst)
		return
	}
	defer r.Close()
	m, err := metainfo.Load(r)
	if err != nil {
		logrus.WithError(err).Errorf("Couldn't generate torrent of %v", dgst)
		return
	}
	// TODO: find registry host and http(s)
	m.Announce = "http://registry/bittorrent/announce/" + dgst.String()
	buff := bytes.NewBuffer(nil)
	m.Write(buff)
	// TODO: add mutex sync
	torrents[dgst] = buff.Bytes()
}
