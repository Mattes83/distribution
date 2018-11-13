package storage

import (
	"context"
	"net/http"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	digest "github.com/opencontainers/go-digest"
)

// Implement distribution.Namespace using DB APIs

type rdbRegistry struct {
	distribution.Namespace
	rdb RepoMetadataDB
	//driver driver.StorageDriver
	bs distribution.BlobStore
}

func NewRDBRegistry(sd driver.StorageDriver) (distribution.Namespace, error) {
	statter := &blobStatter{
		driver: driver,
	}
	bs := &blobStore{
		driver:  driver,
		statter: statter,
	}
	return &rdbRegistry{
		rdb: rdb,
		bs:  bs,
	}, nil
}

func (r *registry) Scope() distribution.Scope {
	return distribution.GlobalScope
}

func (r *registry) Repository(ctx context.Context, name reference.Named) (distribution.Repository, error) {
	return &rdbRepo{reg: registry, name: name, rdb: rdb}, nil
}

func (r *registry) Repositories(ctx context.Context, repos []string, last string) (n int, err error) {
	panic("not implemented")
}

func (r *registry) Blobs() distribution.BlobEnumerator {
	panic("not implemented")
}

func (r *registry) BlobStatter() distribution.BlobStatter {
	panic("not implemented")
}

// --- repo
type rdbRepo struct {
	distribution.Repository
	reg  *registry
	name reference.Name
	rdb  RepoMetadataDB
}

func (r *rdbRepo) Named() reference.Named {
	return r.name
}

func (r *rdbRepo) Manifests(ctx context.Context, options ...distribution.ManifestServiceOption) (distribution.ManifestService, error) {
	return &rdbManifestSvc{
		repo: r,
		rdb:  rdb,
	}, nil
}

func (r *rdbRepo) Blobs(ctx context.Context) distribution.BlobStore {
	statter := &blobStatter{
		driver: driver,
	}
	bs := &blobStore{
		driver:  driver,
		statter: statter,
	}
	return &rdbBlobstore{
		fileBlobstore: bs,
		repo:          r,
	}
}

func (r *rdbRepo) Tags(ctx context.Context) distribution.TagService {
	return &rdbTagsvc{
		repo: r,
		rdb:  r.rdb,
	}
}

// --- manifests
type rdbManifestSvc struct {
	distribution.ManifestService
	repo *rdbRepo
	rdb  RepoMetadataDB
}

func (m *rdbManifestSvc) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	return m.rdb.IsManifestLinked(ctx, m.repo.name.Name(), dgst)
}

func (m *rdbManifestSvc) Get(ctx context.Context, dgst digest.Digest, options ...distribution.ManifestServiceOption) (distribution.Manifest, error) {
	dbm, err := m.rdb.GetRepoManifest(ctx, m.repo.name.Name(), dgst)
	if err != nil {
		return nil, err
	}
	return dbManifestToDistribManifest(dbm), nil
}

func (m *rdbManifestSvc) Put(ctx context.Context, manifest distribution.Manifest, options ...distribution.ManifestServiceOption) (digest.Digest, error) {
	dbm, err := distribManifestToRDBManifest(manifest)
	if err != nil {
		return nil, err
	}
	err = rdb.PutManifestWithRepo(ctx, dbm, m.repo.name.Name())
	if err != nil {
		return nil, err
	}
	return dbm.Blob.Digest, nil
}

func (m *rdbManifestSvc) Delete(ctx context.Context, dgst digest.Digest) error {
	panic("not implemented")
}

// --- blobs
type rdbBlobstore struct {
	distribution.BlobStore
	fileBlobstore distribution.BlobStore
	repo          *rdbRepo
	rdb           RepoMetadataDB
}

func (r *rdbBlobstore) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	// TODO: Check if this gets called for manifest digest
	linked, err := r.rdb.IsBlobLinked(ctx, dgst, r.repo.name.Name())
	if err != nil {
		return distribution.Descriptor{}, err
	}
	if !exists {
		return distribution.Descriptor{}, distribuion.ErrBlobUnknown
	}
	return r.fileBlobstore.Stat(ctx, dgst)
}

func (r *rdbBlobstore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	linked, err := r.rdb.IsBlobLinked(ctx, dgst, r.repo.name.Name())
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, distribuion.ErrBlobUnknown
	}
	return r.fileBlobstore.Get(ctx, dgst)
}

func (r *rdbBlobstore) Open(ctx context.Context, dgst digest.Digest) (distribution.ReadSeekCloser, error) {
	linked, err := r.rdb.IsBlobLinked(ctx, dgst, r.repo.name.Name())
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, distribuion.ErrBlobUnknown
	}
	return r.fileBlobstore.Open(ctx, dgst)
}

func (r *rdbBlobstore) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	desc, err := r.fileBlobstore.Put(ctx, mediaType, p)
	if err != nil {
		return desc, err
	}
	err = r.rdb.PutBlobWithRepo(ctx, descriptorToBlob(desc), r.repo.name.Name())
	if err != nil {
		return distribution.Descriptor{}, err
	}
	return desc, nil
}

func (r *rdbBlobstore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	return r.fileBlobstore.Create(ctx, options...)
}

func (r *rdbBlobstore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	return r.fileBlobstore.Resume(ctx, id)
}

func (r *rdbBlobstore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	// TODO: Verify it doesnt get called for manifest
	return r.fileBlobstore.ServeBlob(ctx, w, r)
}

func (r *rdbBlobstore) Delete(ctx context.Context, dgst digest.Digest) error {
	panic("not implemented")
}

// --- Tags
type rdbTagsvc struct {
	distribution.TagService
	repo *rdbRepo
	rdb  RepoMetadataDB
}

func (r *rdbTagsvc) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {
	m, err := r.rdb.GetTagManifest(ctx, r.repo.name.Name(), tag)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	return dbManifestToDescriptor(m), nil
}

func (r *rdbTagsvc) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	return r.rdb.LinkTag(ctx, r.repo.name.Name(), tag, descriptorToManifest(desc))
}

func (r *rdbTagsvc) Untag(ctx context.Context, tag string) error {
	return r.rdb.DeleteTag(ctx, r.repo.name.Name(), tag)
}

func (r *rdbTagsvc) All(ctx context.Context) ([]string, error) {
	panic("not implemented")
}

func (r *rdbTagsvc) Lookup(ctx context.Context, digest distribution.Descriptor) ([]string, error) {
	panic("not implemented")
}
