// Package torrent - returns bittorrent file w.r.t blob content

package torrent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
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
	tracker string
}

func NewTorrentRepository(r distribution.Repository, tracker string) distribution.Repository {
	return repository{Repository: r, tracker: tracker}
}

type torrentBlobStore struct {
	distribution.BlobStore
	tracker string
}

func (r repository) Blobs(ctx context.Context) distribution.BlobStore {
	return torrentBlobStore{
		BlobStore: r.Repository.Blobs(ctx),
		tracker:   r.tracker,
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
		go func() {
			if err := createTorrentFile(torrents, t.tracker, t.BlobStore, dgst); err != nil {
				dcontext.GetLogger(ctx).WithError(err).Error("error creating torrent")
			} else {
				torrent = torrents[dgst]
			}
		}()
		return t.BlobStore.ServeBlob(ctx, w, r, dgst)
	}
	w.Header().Set("Content-Type", "application/x-bittorrent")
	w.Write(torrent)
	return nil
}

func createTorrentFile(torrents TorrentStore, tracker string, bs distribution.BlobStore, dgst digest.Digest) error {
	// TODO: check if torrent generation is already progress before continuing
	logger := logrus.WithField("digest", dgst.String())
	ctx := context.Background()

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
	defer os.Remove(path)
	io.Copy(f, r)
	f.Close()
	s, _ := os.Stat(path)
	logger.Infof("size is %d", s.Size())

	// generate torrent from that file
	//b := v2.NewURLBuilderFromRequest(req, true)
	//announceURL, _ := b.BuildURLAppendingPath("/bittorrent/announce") // TODO: Find a place for this hard-coded path
	mi := metainfo.MetaInfo{
		Announce: tracker,
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

	// add torrent to store and start seeding it. This is not a scalable solution. Ideally we want only engines to seed
	// TODO: add mutex sync
	torrents[dgst] = buff.Bytes()
	t, err := client.AddTorrent(&mi)
	if err != nil {
		return err
	}
	go t.DownloadAll()
	return nil
}

func extractAndStorePeer(r *http.Request) (torrent.Peers, error) {
	// get info_hash
	q := r.URL.Query()
	ihStr := q.Get("info_hash")
	if ihStr == "" {
		return nil, errors.New("no/empty info_hash")
	}
	ih := metainfo.NewHashFromHex(ihStr)

	// extract requesting peer
	peerID := q.Get("peer_id")
	if peerID == "" {
		return nil, errors.New("no/empty peer_id")
	}
	peerIDBytes := []byte(peerID)
	if len(peerIDBytes) != 20 {
		return nil, errors.New("peer_id len!=20")
	}
	var peerID20Bytes [20]byte
	for i, b := range peerIDBytes {
		peerID20Bytes[i] = b
	}
	ipStr := q.Get("ip")
	if ipStr == "" {
		// ip is optional but i am failing since i don't know how tracker would otherwise give out peer info
		return nil, errors.New("no/empty ip")
	}
	portStr := q.Get("port")
	if portStr == "" {
		// port is optional but i am failing since i don't know how tracker would otherwise give out peer info
		return nil, errors.New("no/empty port")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	peer := torrent.Peer{
		Id:                 peerID20Bytes,
		IP:                 net.ParseIP(ipStr),
		Port:               port,
		Source:             "",
		SupportsEncryption: false,
		PexPeerFlags:       0,
	}

	// add requesting peer to cache
	peerCache.AddPeer(ih, peer, 3*time.Hour)

	// get all peers based on info_hash
	peers, err := peerCache.GetPeers(ih)
	if err != nil {
		return nil, err
	}

	// return bencoded peers
	return peers, nil
}

type AnnounceResponse struct {
	Interval time.Duration `bencode:"internval"`
	Peers    []Peer        `bencode:"peers"`
}

type Peer struct {
	ID [20]byte `bencode:"id"`
	IP string   `bencode:`
}

// TrackerAnnounceHandler responds to announce requests from peers; acts as tracker
// TODO: Move to another package. Currently all torrent code is jammed in here
func TrackerAnnounceHandler(w http.ResponseWriter, r *http.Request) {
	//peers, err := extractAndStorePeer(r)
	//if err != nil {
	//	logrus.WithError(err).Error("error extracting peer info")
	//	w.Write([]byte(err.Error()))
	//	w.WriteHeader(http.StatusInternalServerError)
	//}
	w.Write([]byte("test"))
	w.WriteHeader(http.StatusOK)
}

func init() {
	var err error
	cfg := torrent.NewDefaultClientConfig()
	cfg.Seed = true
	cfg.DataDir = "/tmp"
	cfg.NoDHT = true
	client, err = torrent.NewClient(cfg)
	if err != nil {
		logrus.WithError(err).Error("Couldnt create client")
	}
}
