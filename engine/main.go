// simulates an engine http server that only pulls images via bittorrent. It doesn't do anything else
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"

	"github.com/sirupsen/logrus"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/docker/distribution"

	"github.com/docker/distribution/registry/client"

	"github.com/anacrolix/torrent"
	"github.com/opencontainers/go-digest"
)

func main() {
	regURL := os.Getenv("REGISTRY_URL")
	if regURL == "" {
		panic("no REGISTRY_URL")
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		panic("no DATA_DIR")
	}
	runHTTPServer(NewEngine(regURL, dataDir))
}

func runHTTPServer(e Engine) {
	s := httpServer{e}
	r := mux.NewRouter()
	r.HandleFunc("/repos/{repo}/tags/{tag}/pull", s.PullImage)
	r.HandleFunc("/repos/{repo}/layers/{digest}/status", s.GetDownloadStatus)
	logrus.WithError(http.ListenAndServe(":80", r)).Fatal("error listening server")
}

type httpServer struct {
	Engine
}

func (s *httpServer) PullImage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	err := s.Pull(context.Background(), vars["repo"], vars["tag"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *httpServer) GetDownloadStatus(w http.ResponseWriter, r *http.Request) {
}

type LayerStatus struct {
	Percentage int
}

type Engine interface {
	Pull(ctx context.Context, repo, tag string) error
	Download(ctx context.Context, repo string, dgst digest.Digest) error
	GetDownloadStatus(ctx context.Context, dgst digest.Digest) (LayerStatus, error)
}

type engine struct {
	tc *torrent.Client
	//regClient client.Registry
	regURL  string
	dataDir string
	logger  *logrus.Entry
}

func NewEngine(regURL, dataDir string) Engine {
	cfg := torrent.NewDefaultClientConfig()
	cfg.NoDHT = true
	cfg.DataDir = dataDir
	cfg.Seed = true
	tc, err := torrent.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	return &engine{
		tc:      tc,
		regURL:  regURL,
		dataDir: dataDir,
		logger:  logrus.New().WithField("module", "engine"),
	}
}

type repoName string

func (r repoName) Name() string {
	return string(r)
}

func (r repoName) String() string {
	return string(r)
}

func (e *engine) getDistribRepo(ctx context.Context, repo string) (distribution.Repository, error) {
	//repoName, err := reference.ParseNamed(repo)
	//if err != nil {
	//	return nil, err
	//}
	repoObj, err := client.NewRepository(repoName(repo), e.regURL, nil)
	if err != nil {
		return nil, err
	}
	return repoObj, nil
}

func (e *engine) Pull(ctx context.Context, repo, tag string) error {
	// get manifest referred by tag
	repoObj, err := e.getDistribRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get repo error: %w", err)
	}
	desc, err := repoObj.Tags(ctx).Get(ctx, tag)
	if err != nil {
		return fmt.Errorf("tag.get error: %w", err)
	}
	ms, err := repoObj.Manifests(ctx)
	if err != nil {
		return fmt.Errorf("manifestservice error: %w", err)
	}
	m, err := ms.Get(ctx, desc.Digest)
	if err != nil {
		return fmt.Errorf("manifest.get %s error: %w", desc.Digest, err)
	}

	// start downloading layers in the manifest
	layers := m.References()
	for _, layer := range layers {
		go e.Download(ctx, repo, layer.Digest)
	}
	return nil
}

func (e *engine) Download(ctx context.Context, repo string, dgst digest.Digest) error {
	// Send blob download request advertising support for bittorrent
	blobURL := fmt.Sprintf("%s/v2/%s/blobs/%s", e.regURL, repo, dgst)
	req, err := http.NewRequest("GET", blobURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/x-bittorrent")
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	// check if response is torrent
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("invalid blob response status code: %d", resp.StatusCode))
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "application/x-bittorrent" {
		e.logger.WithField("digest", dgst).Info("Downloading as torrent")
		return e.downloadTorrent(ctx, resp.Body)
	}

	// not torrent; download normally
	e.logger.WithField("digest", dgst).Info("Downloading manually")
	out, err := os.Create(e.digestPath(dgst))
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

func (e *engine) digestPath(dgst digest.Digest) string {
	return fmt.Sprintf("%s/%s.tar.gz", e.dataDir, dgst.String())
}

func (e *engine) downloadTorrent(ctx context.Context, torrentContent io.ReadCloser) error {
	info, err := metainfo.Load(torrentContent)
	if err != nil {
		return err
	}
	t, err := e.tc.AddTorrent(info)
	if err != nil {
		return err
	}
	t.DownloadAll()
	return nil
}

func (e *engine) GetDownloadStatus(ctx context.Context, dgst digest.Digest) (LayerStatus, error) {
	return LayerStatus{0}, nil
}