package rdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
)

type BlobNotFoundErr struct {
	Digest digest.Digest
}

func (e BlobNotFoundErr) Error() string {
	return fmt.Sprintf("Blob not found: %s", e.Digest)
}

type ManifestNotFoundErr struct {
	Digest digest.Digest
}

func (e ManifestNotFoundErr) Error() string {
	return fmt.Sprintf("Manifest not found: %s", e.Digest)
}

// Blob is registry image layer or config json
type Blob struct {
	Digest       digest.Digest
	Refcount     int64
	Size         int64
	LastReferred time.Time
}

func (b *Blob) String() string {
	return fmt.Sprintf("Blob digest: %s, refcount: %d, size: %d", b.Digest, b.Refcount, b.Size)
}

// Manifest is image manifest that composes of layer blobs or other manifests
type Manifest struct {
	Blob
	Refers  []*Blob
	Payload []byte
}

func (m *Manifest) String() string {
	refers := []string{}
	for _, b := range m.Refers {
		refers = append(refers, b.String())
	}
	return fmt.Sprintf("Manifest %s, refers: [%s]", m.Blob.String(), strings.Join(refers, ","))
}

// RepoMetadataDB is Database interface to access repository metadata (i.e. repos and blobs) in
// registry from a relational database
type RepoMetadataDB interface {

	// PutBlobWithRepo inserts the given blob in DB if it is not there. It also inserts the repo
	// associated with the blob and links it.
	PutBlobWithRepo(ctx context.Context, blob *Blob, repo string) error

	// PutManifestOnRepo updates the manifest information (refers field) and increments referred blob's
	// refcount only if manifest is not updated already. It then links the manifest to the repo.
	// This is slightly different from PutManifestWith(BlobsAnd)Repo where it expects the manifest
	// and referred blobs to already exist in the DB
	PutManifestOnRepo(ctx context.Context, manifest *Manifest, repo string) error

	// GetRepoManifest returns manifest on a repo
	GetRepoManifest(ctx context.Context, repo string, dgst digest.Digest) (*Manifest, error)

	// LinkTag links the given tag with the manifest provided in the given repository. The manifest
	// must be already linked to the repo. The manifests's refcount is incremented
	LinkTag(ctx context.Context, repo string, tag string, manifest digest.Digest) error

	// GetTagManifest gets manifest pointed to by given tag
	GetTagManifest(ctx context.Context, repo string, tag string) (*Manifest, error)

	// DeleteTag will unlink all the manifests associated with the tag and delete it. It will
	// decrement all the linked manifests' refcount
	DeleteTag(ctx context.Context, repo string, tag string) error

	// Delete the repository by deleting all tags and associated manifests with it. The refcounts of
	// linked manifests is decremented
	DeleteRepo(ctx context.Context, repo string) error

	// IsManifestLinked returns whether a given manifest is linked to the repo
	IsManifestLinked(ctx context.Context, repo string, dgst digest.Digest) (bool, error)

	// IsBlobLinked returns whether a given blob is linked to the repo
	IsBlobLinked(ctx context.Context, repo string, dgst digest.Digest) (bool, error)
}
