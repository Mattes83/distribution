package pg

// This file contains integration tests for code in pg.go that talks to a Postgres container

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/docker/distribution/registry/storage/rdb"
	"github.com/docker/distribution/uuid"
	pq "github.com/lib/pq"
	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

var (
	pgdb   *pgDB = nil
	pgLock       = &sync.Mutex{}
)

func newPGDB(t *testing.T) *pgDB {
	migrationsDir := os.Getenv("GOPATH") + "/src/github.com/docker/distribution/registry/storage/rdb/pg/migrations"
	pglocal, err := NewDB(context.Background(), os.Getenv("PGURL"), migrationsDir)
	require.NoError(t, err)
	return pglocal.(*pgDB)
}

func getPGDB(t *testing.T) *pgDB {
	pgLock.Lock()
	defer pgLock.Unlock()
	if pgdb != nil {
		return pgdb
	}
	pgdb = newPGDB(t)
	return pgdb
}

func getBlob(t *testing.T, pg *pgDB, dgst digest.Digest) *rdb.Blob {
	sess := pg.conn.NewSession(nil)
	var blob rdb.Blob
	err := sess.Select("digest", "refcount", "size", "last_referred").From("blobs").Where("digest=?", dgst).LoadOne(&blob)
	require.NoError(t, err, fmt.Sprintf("getBlob %v", err))
	// reformat to append UTC timezone which the PG driver has stripped out
	blob.LastReferred = blob.LastReferred.UTC()
	return &blob
}

func getManifest(t *testing.T, pg *pgDB, dgst digest.Digest) *rdb.Manifest {
	blob := getBlob(t, pg, dgst)
	sess := pg.conn.NewSession(nil)
	manifest := rdb.Manifest{Blob: *blob}
	var referDgsts []string
	err := sess.Select("refers").From("blobs").Where("digest=?", dgst).LoadOne(pq.Array(&referDgsts))
	require.NoError(t, err)
	err = sess.Select("payload").From("blobs").Where("digest=?", dgst).LoadOne(&manifest.Payload)
	require.NoError(t, err)
	for _, referDgst := range referDgsts {
		manifest.Refers = append(manifest.Refers, getBlob(t, pg, digest.Digest(referDgst)))
	}
	return &manifest
}

func insertManifest(t *testing.T, pg *pgDB, manifest *rdb.Manifest) {
	sess := pg.conn.NewSession(nil)
	for _, blob := range manifest.Refers {
		require.NoError(t, pg.PutBlob(context.Background(), blob))
	}
	// insert manifest
	mb := &manifest.Blob
	_, err := sess.InsertInto("blobs").
		Columns("digest", "size", "refcount", "last_referred", "refers").
		Values(mb.Digest, mb.Size, mb.Refcount, mb.LastReferred, pq.Array(digests(manifest.Refers))).
		Exec()
	require.NoError(t, err)
}

type RepoRow struct {
	ID        int64
	FullName  string
	Namespace string
	Name      string
}

func getRepo(pg *pgDB, name string) (*RepoRow, error) {
	sess := pg.conn.NewSession(nil)
	var repo RepoRow
	err := sess.Select("id", "full_name", "namespace", "name").From("repos").Where("full_name=?", name).LoadOne(&repo)
	return &repo, err
}

func createRepo(t *testing.T, pg *pgDB, fullName string, namespace string, name string) int64 {
	sess := pg.conn.NewSession(nil)
	var repoId int64
	err := sess.SelectBySql(
		`INSERT INTO repos(full_name, namespace, name)
		 VALUES(?, ?, ?)
		 RETURNING id`,
		fullName, namespace, name).LoadOne(&repoId)
	require.NoError(t, err)
	return repoId
}

func insertSampleRepo(t *testing.T, pg *pgDB) *RepoRow {
	ns, name, fn := sampleRepo()
	_ = createRepo(t, pg, fn, ns, name)
	row, err := getRepo(pg, fn)
	require.NoError(t, err)
	return row
}

type RepoLinkRow struct {
	RepoId string
	Digest string
}

func isRepoLinked(pg *pgDB, repo_id int64, dgst digest.Digest) bool {
	sess := pg.conn.NewSession(nil)
	var row RepoLinkRow
	err := sess.Select("repo_id", "digest").From("repos_blobs").Where("repo_id=? AND digest=?", repo_id, dgst).LoadOne(&row)
	return err == nil
}

func assertRepo(t *testing.T, pg *pgDB, fullName string, namespace string, name string) *RepoRow {
	repo, err := getRepo(pg, fullName)
	require.NoError(t, err)
	require.Equal(t, fullName, repo.FullName)
	require.Equal(t, namespace, repo.Namespace)
	require.Equal(t, name, repo.Name)
	return repo
}

func sampleRepo() (ns string, name string, fullName string) {
	name = fmt.Sprintf("name%s", uuid.Generate().String())
	return "namespace", name, "namespace/" + name
}

func TestPutNewBlobWithRepo(t *testing.T) {
	t.Parallel()
	_, _ = testPutNewBlobWithRepo(t, getPGDB(t))
}

// New blob with new repo is added to DB
func testPutNewBlobWithRepo(t *testing.T, pg *pgDB) (*RepoRow, *rdb.Blob) {
	// put new blob
	blob := rdb.Blob{Digest: digest.Digest(uuid.Generate().String())}
	ns, name, repoName := sampleRepo()
	require.NoError(t, pg.PutBlobWithRepo(context.Background(), &blob, repoName))
	gotBlob := getBlob(t, pg, blob.Digest)
	require.Equal(t, blob.Digest, gotBlob.Digest)
	require.EqualValues(t, 0, gotBlob.Refcount)
	require.WithinDuration(t, time.Now().UTC(), gotBlob.LastReferred, 100*time.Millisecond)
	// check repo got created
	repo := assertRepo(t, pg, repoName, ns, name)
	// check repo link got created
	require.True(t, isRepoLinked(pg, repo.ID, blob.Digest))
	return repo, gotBlob
}

func testPutNewManifestOnRepo(t *testing.T, pg *pgDB) (*rdb.Manifest, *RepoRow) {
	// create repo and blob before adding manifest
	repo, blob := testPutNewBlobWithRepo(t, pg)
	ctx := context.Background()
	now := time.Now().UTC()
	//existingBlob := rdb.Blob{
	//	Digest:       digest.Digest(uuid.Generate().String()),
	//	Size:         20,
	//	Refcount:     10,
	//	LastReferred: now.Add(-1 * time.Hour),
	//}
	//require.NoError(t, pg.PutBlob(ctx, &existingBlob))
	// create sample manifest
	manifest := rdb.Manifest{
		Blob: rdb.Blob{
			Digest: digest.Digest(uuid.Generate().String()),
			Size:   10,
		},
		Refers: []*rdb.Blob{
			blob,
		},
		Payload: []byte("test"),
	}
	// insert it
	require.NoError(t, pg.PutManifestOnRepo(ctx, &manifest, repo.FullName))
	// check repo link got created
	require.True(t, isRepoLinked(pg, repo.ID, manifest.Blob.Digest))
	// verify manifest got inserted
	gotManifest := getManifest(t, pg, manifest.Blob.Digest)
	require.Equal(t, manifest.Blob.Digest, gotManifest.Blob.Digest)
	require.EqualValues(t, 0, gotManifest.Blob.Refcount)
	require.EqualValues(t, 10, gotManifest.Blob.Size)
	require.WithinDuration(t, now, gotManifest.Blob.LastReferred, 100*time.Millisecond)
	require.EqualValues(t, []byte("test"), gotManifest.Payload)
	// blob refcount is increased and last_referred updated
	require.Equal(t, blob.Digest, gotManifest.Refers[0].Digest)
	require.EqualValues(t, blob.Refcount+1, gotManifest.Refers[0].Refcount)
	require.NotEqual(t, blob.LastReferred, gotManifest.Refers[0].LastReferred)
	require.WithinDuration(t, now, gotManifest.Refers[0].LastReferred, 100*time.Millisecond)
	return gotManifest, repo
}

// New manifest is created on existing repo with refcount 0
// referred blobs are inserted if they do not exist and updated with increased refcount if they do
func TestPutNewManifestOnRepo(t *testing.T) {
	t.Parallel()
	pg := getPGDB(t)
	_, _ = testPutNewManifestOnRepo(t, pg)
}

func TestGetTagManifest(t *testing.T) {
	t.Parallel()
	pg := getPGDB(t)
	manifest, repo := testPutNewManifestOnRepo(t, pg)
	ctx := context.Background()
	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", manifest.Digest))
	gotManifest, err := pg.GetTagManifest(ctx, repo.FullName, "latest")
	require.NoError(t, err)
	require.EqualValues(t, manifest.Digest, gotManifest.Digest)
	require.EqualValues(t, manifest.Size, gotManifest.Size)
	require.EqualValues(t, manifest.Payload, gotManifest.Payload)
}
