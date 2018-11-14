package mem

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/docker/distribution/registry/storage/rdb"
	digest "github.com/opencontainers/go-digest"
)

// Inmemory impl of RepoMetadataDB

type memDB struct {
	rdb.RepoMetadataDB
	repoBlobs     map[string]map[digest.Digest]*rdb.Blob
	repoManifests map[string]map[digest.Digest]*rdb.Manifest
	tags          map[string]*rdb.Manifest
}

func New() rdb.RepoMetadataDB {
	return &memDB{
		repoBlobs:     make(map[string]map[digest.Digest]*rdb.Blob),
		repoManifests: make(map[string]map[digest.Digest]*rdb.Manifest),
		tags:          make(map[string]*rdb.Manifest),
	}
}

func (m *memDB) String() string {
	b, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func (m *memDB) PutBlobWithRepo(ctx context.Context, blob *rdb.Blob, repo string) error {
	blobs := m.repoBlobs[repo]
	if blobs == nil {
		m.repoBlobs[repo] = make(map[digest.Digest]*rdb.Blob)
	}
	m.repoBlobs[repo][blob.Digest] = blob
	return nil
}

func (m *memDB) PutManifestOnRepo(ctx context.Context, manifest *rdb.Manifest, repo string) error {
	manifests := m.repoManifests[repo]
	if manifests == nil {
		m.repoManifests[repo] = make(map[digest.Digest]*rdb.Manifest)
	}
	m.repoManifests[repo][manifest.Blob.Digest] = manifest
	return nil
}

func (m *memDB) GetRepoManifest(ctx context.Context, repo string, dgst digest.Digest) (*rdb.Manifest, error) {
	manifests := m.repoManifests[repo]
	if manifests == nil {
		return nil, rdb.ManifestNotFoundErr{Digest: dgst}
	}
	manifest, exists := manifests[dgst]
	if !exists {
		return nil, rdb.ManifestNotFoundErr{Digest: dgst}
	}
	return manifest, nil
}

func (m *memDB) LinkTag(ctx context.Context, repo string, tag string, dgst digest.Digest) error {
	manifest, err := m.GetRepoManifest(ctx, repo, dgst)
	if err != nil {
		return err
	}
	m.tags[repo+":"+tag] = manifest
	return nil
}

func (m *memDB) GetTagManifest(ctx context.Context, repo string, tag string) (*rdb.Manifest, error) {
	manifest, exists := m.tags[repo+":"+tag]
	if !exists {
		return nil, fmt.Errorf("tag not found: %s", tag)
	}
	return manifest, nil
}

func (m *memDB) DeleteTag(ctx context.Context, repo string, tag string) error {
	delete(m.tags, repo+":"+tag)
	return nil
}

func (m *memDB) DeleteRepo(ctx context.Context, repo string) error {
	delete(m.repoBlobs, repo)
	delete(m.repoManifests, repo)
	// TODO: Check if deleting key while enumerating is allowed
	for k, _ := range m.tags {
		if strings.HasPrefix(k, repo) {
			delete(m.tags, k)
		}
	}
	return nil
}

func (m *memDB) IsManifestLinked(ctx context.Context, repo string, dgst digest.Digest) (bool, error) {
	_, exists := m.repoManifests[repo][dgst]
	return exists, nil
}

func (m *memDB) IsBlobLinked(ctx context.Context, repo string, dgst digest.Digest) (bool, error) {
	_, exists := m.repoBlobs[repo][dgst]
	return exists, nil
}
