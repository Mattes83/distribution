// Package torrent - returns bittorrent file w.r.t blob content

package torrent

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
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
	client    *torrent.Client
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
		err = createTorrentFile(ctx, torrents, t.BlobStore, dgst)
		if err != nil {
			return t.BlobStore.ServeBlob(ctx, w, r, dgst)
		}
		torrent = torrents[dgst]
	}
	w.Header().Set("Content-Type", "application/x-bittorrent")
	w.Write(torrent)
	return nil
}

func createTorrentFile(ctx context.Context, torrents TorrentStore, bs distribution.BlobStore, dgst digest.Digest) error {
	// TODO: check if torrent generation is already progress before continuing
	logger := logrus.WithField("digest", dgst.String())
	// get digest reader
	r, err := bs.Open(ctx, dgst)
	if err != nil {
		logger.WithError(err).Error("Couldn't get content to generate torrent")
		return err
	}
	defer r.Close()
	// write it to temp file
	path := "/tmp/" + dgst.String() + ".tar.gz"
	f, err := os.Create(path)
	if err != nil {
		logger.WithError(err).Error("Couldn't create temp file")
		return err
	}
	io.Copy(f, r)
	f.Close()
	s, _ := os.Stat(path)
	logger.Infof("size is %d", s.Size())
	// generate torrent from that file
	mi := metainfo.MetaInfo{
		Announce: "http://terriblecode.com:6969/announce",
	}
	mi.SetDefaults()
	info := metainfo.Info{
		Name:        dgst.String(),
		PieceLength: 256 * 1024,
	}
	err = info.BuildFromFilePath(path)
	if err != nil {
		logger.WithError(err).Error("Couldn't generate from file")
		return err
	}
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		logger.WithError(err).Error("Couldn't marshal")
		return err
	}
	buff := bytes.NewBuffer(nil)
	mi.Write(buff)
	// TODO: add mutex sync
	torrents[dgst] = buff.Bytes()
	client.AddTorrent(&mi)
	return nil
}

func init() {
	var err error
	client, err = torrent.NewClient(&torrent.Config{DataDir: "/tmp", Seed: true})
	if err != nil {
		logrus.WithError(err).Error("Couldnt create client")
	}
	go func() {
		client.WaitAll()
	}()
}
