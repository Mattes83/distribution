package storage

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/rdb"
	"github.com/docker/distribution/registry/storage/rdb/pg"
	digest "github.com/opencontainers/go-digest"
)

// Implement distribution.Namespace using DB APIs

type rdbRegistry struct {
	distribution.Namespace
	db     rdb.RepoMetadataDB
	driver driver.StorageDriver
}

func NewRDBRegistry(ctx context.Context, sd driver.StorageDriver, dsn string) (distribution.Namespace, error) {
	migrationsDir := os.Getenv("GOPATH") + "/src/github.com/docker/distribution/registry/storage/rdb/pg/migrations"
	db, err := pg.NewDB(ctx, dsn, migrationsDir)
	if err != nil {
		return nil, err
	}
	return &rdbRegistry{
		db:     db,
		driver: sd,
	}, nil
}

func (r *rdbRegistry) Scope() distribution.Scope {
	return distribution.GlobalScope
}

func (r *rdbRegistry) Repository(ctx context.Context, name reference.Named) (distribution.Repository, error) {
	return &rdbRepo{
		reg:  r,
		name: name,
		db:   r.db,
	}, nil
}

func (r *rdbRegistry) Repositories(ctx context.Context, repos []string, last string) (n int, err error) {
	panic("not implemented")
}

func (r *rdbRegistry) Blobs() distribution.BlobEnumerator {
	panic("not implemented")
}

func (r *rdbRegistry) BlobStatter() distribution.BlobStatter {
	panic("not implemented")
}

// --- repo
type rdbRepo struct {
	distribution.Repository
	reg  *rdbRegistry
	name reference.Named
	db   rdb.RepoMetadataDB
}

func (r *rdbRepo) Named() reference.Named {
	return r.name
}

func (r *rdbRepo) Manifests(ctx context.Context, options ...distribution.ManifestServiceOption) (distribution.ManifestService, error) {
	return &rdbManifestSvc{
		repo: r,
		db:   r.db,
	}, nil
}

func (r *rdbRepo) Blobs(ctx context.Context) distribution.BlobStore {
	bs := &blobStore{
		driver: r.reg.driver,
		statter: &blobStatter{
			driver: r.reg.driver,
		},
	}
	var statter distribution.BlobDescriptorService = &linkedBlobStatter{
		blobStore:   bs,
		repository:  r,
		linkPathFns: []linkPathFunc{blobLinkPath},
	}
	fileBS := &linkedBlobStore{
		registry:  r.reg,
		blobStore: bs,
		blobServer: &blobServer{
			driver:  r.reg.driver,
			statter: statter,
			pathFn:  bs.path,
		},
		blobAccessController: statter,
		repository:           r,
		ctx:                  ctx,

		// TODO(stevvooe): linkPath limits this blob store to only layers.
		// This instance cannot be used for manifest checks.
		linkPathFns: []linkPathFunc{blobLinkPath},
	}
	return &rdbBlobstore{
		fileBlobstore: fileBS,
		repo:          r,
		db:            r.db,
	}
}

func (r *rdbRepo) Tags(ctx context.Context) distribution.TagService {
	return &rdbTagsvc{
		repo: r,
		db:   r.db,
	}
}

// --- manifests
type rdbManifestSvc struct {
	distribution.ManifestService
	repo *rdbRepo
	db   rdb.RepoMetadataDB
}

func (m *rdbManifestSvc) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	dcontext.GetLogger(ctx).Debugf("rdbManifest.Exists: %s", dgst)
	return m.db.IsManifestLinked(ctx, m.repo.name.Name(), dgst)
}

func (m *rdbManifestSvc) Get(ctx context.Context, dgst digest.Digest, options ...distribution.ManifestServiceOption) (distribution.Manifest, error) {
	dcontext.GetLogger(ctx).Debugf("rdbManifest.Get: %s", dgst)
	dbm, err := m.db.GetRepoManifest(ctx, m.repo.name.Name(), dgst)
	if err != nil {
		return nil, err
	}
	distribm, err := dbManifestToDistribManifest(dbm)
	if err != nil {
		return nil, err
	}
	return distribm, nil
}

func dbManifestToDistribManifest(m *rdb.Manifest) (distribution.Manifest, error) {
	// Hardcoding for schema2
	dm, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, m.Payload)
	if err != nil {
		return nil, err
	}
	return dm, nil
}

func (m *rdbManifestSvc) Put(ctx context.Context, manifest distribution.Manifest, options ...distribution.ManifestServiceOption) (digest.Digest, error) {
	dcontext.GetLogger(ctx).Debug("rdbManifest.Put")
	dbm, err := distribManifestToRDBManifest(manifest)
	if err != nil {
		return "", err
	}
	err = m.db.PutManifestOnRepo(ctx, dbm, m.repo.name.Name())
	if err != nil {
		return "", err
	}
	return dbm.Digest, nil
}

func distribManifestToRDBManifest(dm distribution.Manifest) (*rdb.Manifest, error) {
	mt, payload, err := dm.Payload()
	if err != nil {
		return nil, err
	}
	if mt != schema2.MediaTypeManifest {
		return nil, fmt.Errorf("only schema2 supported: %s", mt)
	}
	_, desc, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, payload)
	if err != nil {
		return nil, err
	}
	refers := dm.References()
	dbm := rdb.Manifest{
		Blob: rdb.Blob{
			Digest: desc.Digest,
			Size:   desc.Size,
		},
		Payload: payload,
		Refers:  make([]*rdb.Blob, len(refers)),
	}
	for i := 0; i < len(refers); i++ {
		dbm.Refers[i] = descriptorToBlob(refers[i])
	}
	return &dbm, nil
}

func (m *rdbManifestSvc) Delete(ctx context.Context, dgst digest.Digest) error {
	panic("not implemented")
}

// --- blobs
type rdbBlobstore struct {
	distribution.BlobStore
	fileBlobstore distribution.BlobStore
	repo          *rdbRepo
	db            rdb.RepoMetadataDB
}

func (r *rdbBlobstore) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	// TODO: Check if this gets called for manifest digest
	dcontext.GetLogger(ctx).Debugf("rdbBlobstore.Stat: %s", dgst)
	linked, err := r.db.IsBlobLinked(ctx, r.repo.name.Name(), dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	if !linked {
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}
	return r.fileBlobstore.Stat(ctx, dgst)
}

func (r *rdbBlobstore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	dcontext.GetLogger(ctx).Debugf("rdbBlobstore.Get: %s", dgst)
	linked, err := r.db.IsBlobLinked(ctx, r.repo.name.Name(), dgst)
	if err != nil {
		return nil, err
	}
	if !linked {
		return nil, distribution.ErrBlobUnknown
	}
	return r.fileBlobstore.Get(ctx, dgst)
}

func (r *rdbBlobstore) Open(ctx context.Context, dgst digest.Digest) (distribution.ReadSeekCloser, error) {
	dcontext.GetLogger(ctx).Debugf("rdbBlobstore.Open: %s", dgst)
	linked, err := r.db.IsBlobLinked(ctx, r.repo.name.Name(), dgst)
	if err != nil {
		return nil, err
	}
	if !linked {
		return nil, distribution.ErrBlobUnknown
	}
	return r.fileBlobstore.Open(ctx, dgst)
}

func (r *rdbBlobstore) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	dcontext.GetLogger(ctx).Debug("rdbBlobstore.Put")
	desc, err := r.fileBlobstore.Put(ctx, mediaType, p)
	if err != nil {
		return desc, err
	}
	err = r.db.PutBlobWithRepo(ctx, descriptorToBlob(desc), r.repo.name.Name())
	if err != nil {
		return distribution.Descriptor{}, err
	}
	return desc, nil
}

func descriptorToBlob(desc distribution.Descriptor) *rdb.Blob {
	return &rdb.Blob{
		Digest: desc.Digest,
		Size:   desc.Size,
	}
}

type rdbBlobwriter struct {
	distribution.BlobWriter
	rdbBS *rdbBlobstore
}

func (w *rdbBlobwriter) Commit(ctx context.Context, provisional distribution.Descriptor) (distribution.Descriptor, error) {
	dcontext.GetLogger(ctx).Debug("rdbBlobwriter.Commit")
	desc, err := w.BlobWriter.Commit(ctx, provisional)
	if err != nil {
		return desc, err
	}
	err = w.rdbBS.db.PutBlobWithRepo(ctx, descriptorToBlob(desc), w.rdbBS.repo.name.Name())
	if err != nil {
		return distribution.Descriptor{}, err
	}
	return desc, nil
}

func (r *rdbBlobstore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	dcontext.GetLogger(ctx).Debug("rdbBlobstore.Create")
	bw, err := r.fileBlobstore.Create(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &rdbBlobwriter{bw, r}, nil
}

func (r *rdbBlobstore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	dcontext.GetLogger(ctx).Debugf("rdbBlobstore.Resume: %s", id)
	bw, err := r.fileBlobstore.Resume(ctx, id)
	if err != nil {
		return nil, err
	}
	return &rdbBlobwriter{bw, r}, nil
}

func (bs *rdbBlobstore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	// TODO: Verify it doesnt get called for manifest
	dcontext.GetLogger(ctx).Debugf("rdbBlobstore.ServeBlob: %s", dgst)
	return bs.fileBlobstore.ServeBlob(ctx, w, r, dgst)
}

func (r *rdbBlobstore) Delete(ctx context.Context, dgst digest.Digest) error {
	panic("not implemented")
}

// --- Tags
type rdbTagsvc struct {
	distribution.TagService
	repo *rdbRepo
	db   rdb.RepoMetadataDB
}

func (r *rdbTagsvc) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {
	m, err := r.db.GetTagManifest(ctx, r.repo.name.Name(), tag)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	return dbManifestToDescriptor(m), nil
}

func dbManifestToDescriptor(m *rdb.Manifest) distribution.Descriptor {
	return distribution.Descriptor{
		MediaType: schema2.MediaTypeManifest,
		Digest:    m.Digest,
		Size:      m.Size,
	}
}

func (r *rdbTagsvc) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	return r.db.LinkTag(ctx, r.repo.name.Name(), tag, desc.Digest)
}

func (r *rdbTagsvc) Untag(ctx context.Context, tag string) error {
	return r.db.DeleteTag(ctx, r.repo.name.Name(), tag)
}

func (r *rdbTagsvc) All(ctx context.Context) ([]string, error) {
	panic("not implemented")
}

func (r *rdbTagsvc) Lookup(ctx context.Context, digest distribution.Descriptor) ([]string, error) {
	panic("not implemented")
}
