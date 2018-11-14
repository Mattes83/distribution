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

// If repo gets created between check and creation then that is detected and created
// repo id is used
//func TestPutNewBlobWithRepoCreatedConcurrently(t *testing.T) {
//	pg := newPGDB(t)
//	// create channel to block repo creation after initial check
//	pg.waitRepo = make(chan bool)
//	// put new blob
//	blob := rdb.Blob{Digest: digest.Digest(uuid.Generate().String())}
//	ns, name, repoName := sampleRepo()
//	errChan := make(chan error)
//	go func() {
//		errChan <- pg.PutBlobWithRepo(context.Background(), &blob, repoName)
//	}()
//	// ensure the function waits after checking repo creation
//	<-pg.waitRepo
//	// create repo while funcion is waiting on channel
//	repoId := createRepo(t, pg, repoName, ns, name)
//	// unblock the function and check if new repo id is linked
//	<-pg.waitRepo
//	err := <-errChan
//	// function didnt fail
//	require.NoError(t, err)
//	// check blob
//	gotBlob := getBlob(t, pg, blob.Digest)
//	require.Equal(t, blob.Digest, gotBlob.Digest)
//	require.EqualValues(t, 0, gotBlob.Refcount)
//	require.WithinDuration(t, time.Now().UTC(), gotBlob.LastReferred, 100*time.Millisecond)
//	// check repo link got created with id created above
//	require.True(t, isRepoLinked(pg, repoId, blob.Digest))
//}
//
//// Adding existing blob to a new repo updates its last_referred
//func TestPutExistingBlobWithRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	repo, blob := testPutNewBlobWithRepo(t, pg)
//	require.NoError(t, pg.PutBlobWithRepo(context.Background(), blob, repo.FullName))
//	gotBlob := getBlob(t, pg, blob.Digest)
//	require.EqualValues(t, 0, gotBlob.Refcount)
//	require.Equal(t, blob.LastReferred, gotBlob.LastReferred)
//}
//
//// Adding existing blob to existing repo will not error and not update last_referred
//func TestPutExistingBlobWithNewRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	_, blob := testPutNewBlobWithRepo(t, pg)
//	// simulate a high refcount to test this scenario
//	_, err := pg.conn.NewSession(nil).Update("blobs").Set("refcount", 20).Where(dbr.Eq("digest", blob.Digest)).Exec()
//	require.NoError(t, err)
//	_, _, repoName := sampleRepo()
//	require.NoError(t, pg.PutBlobWithRepo(context.Background(), blob, repoName))
//	gotBlob := getBlob(t, pg, blob.Digest)
//	require.EqualValues(t, 20, gotBlob.Refcount)
//	require.NotEqual(t, blob.LastReferred, gotBlob.LastReferred)
//}
//
//func testPutNewManifestWithBlobsOnNewRepo(t *testing.T, pg *pgDB) (*rdb.Manifest, *RepoRow) {
//	// create a blob before adding manifest to check blob upsert
//	ctx := context.Background()
//	now := time.Now().UTC()
//	existingBlob := rdb.Blob{
//		Digest:       digest.Digest(uuid.Generate().String()),
//		Size:         20,
//		Refcount:     10,
//		LastReferred: now.Add(-1 * time.Hour),
//	}
//	require.NoError(t, pg.PutBlob(ctx, &existingBlob))
//	// create sample manifest
//	manifest := rdb.Manifest{
//		rdb.Blob: rdb.Blob{
//			Digest: digest.Digest(uuid.Generate().String()),
//			Size:   10,
//		},
//		Refers: []*rdb.Blob{
//			&existingBlob,
//			{Digest: digest.Digest(uuid.Generate().String()), Size: 30},
//		},
//	}
//	ns, name, repoName := sampleRepo()
//	// insert it
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(ctx, &manifest, repoName))
//	// check repo got created
//	repo := assertRepo(t, pg, repoName, ns, name)
//	// check repo link got created
//	require.True(t, isRepoLinked(pg, repo.ID, manifest.Blob.Digest))
//	// verify manifest got inserted
//	gotManifest := getManifest(t, pg, manifest.Blob.Digest)
//	require.Equal(t, manifest.Blob.Digest, gotManifest.Blob.Digest)
//	require.EqualValues(t, 0, gotManifest.Blob.Refcount)
//	require.EqualValues(t, 10, gotManifest.Blob.Size)
//	require.WithinDuration(t, now, gotManifest.Blob.LastReferred, 100*time.Millisecond)
//	// existing blob refcount is increased and last_referred updated, it is also linked to repo
//	require.Equal(t, existingBlob.Digest, gotManifest.Refers[0].Digest)
//	require.True(t, isRepoLinked(pg, repo.ID, existingBlob.Digest))
//	require.EqualValues(t, existingBlob.Refcount+1, gotManifest.Refers[0].Refcount)
//	require.NotEqual(t, existingBlob.LastReferred, gotManifest.Refers[0].LastReferred)
//	require.WithinDuration(t, now, gotManifest.Refers[0].LastReferred, 100*time.Millisecond)
//	// new blob got added and got linked to repo
//	require.Equal(t, manifest.Refers[1].Digest, gotManifest.Refers[1].Digest)
//	require.True(t, isRepoLinked(pg, repo.ID, manifest.Refers[1].Digest))
//	require.EqualValues(t, 1, gotManifest.Refers[1].Refcount)
//	require.WithinDuration(t, now, gotManifest.Refers[1].LastReferred, 100*time.Millisecond)
//	// new blob got added and got linked to repo
//	return gotManifest, repo
//}
//
//// New manifest is created with new repo with refcount 0
//// referred blobs are inserted if they do not exist and updated with increased refcount if they do
//func TestPutNewManifestWithBlobsOnNewRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	_, _ = testPutNewManifestWithBlobsOnNewRepo(t, pg)
//}
//
//// If manifest is inserted between first check and subsequent insertion by another process then it
//// is checked again and its last_referred is updated
//func TestPutNewManifestWithBlobsConcurrently(t *testing.T) {
//	pg := newPGDB(t)
//	now := time.Now().UTC()
//	// create sample manifest
//	manifest := sampleManifest()
//	manifest.Refers[0].Refcount = 2
//	manifest.Blob.LastReferred = now.Add(-1 * time.Hour)
//	ns, name, repoName := sampleRepo()
//	// insert it in goroutine and block after initial check
//	pg.waitManifest = make(chan bool)
//	errChan := make(chan error)
//	go func() {
//		errChan <- pg.PutManifestWithBlobsAndRepo(context.Background(), manifest, repoName)
//	}()
//	// ensure the function waits after checking manifest exists
//	<-pg.waitManifest
//	// now insert the manifest
//	insertManifest(t, pg, manifest)
//	// update last_referred to be "1 hour before" to test that subsequent resume updates to current time
//	_, err := pg.conn.NewSession(nil).
//		Update("blobs").
//		Set("last_referred", now.Add(-1*time.Hour)).
//		Where(dbr.Eq("digest", manifest.Blob.Digest)).
//		Exec()
//	require.NoError(t, err)
//	// and resume the function; it should detect the insertion and just update last_referred
//	<-pg.waitManifest
//	require.NoError(t, <-errChan)
//	// check repo link got created
//	repo := assertRepo(t, pg, repoName, ns, name)
//	require.True(t, isRepoLinked(pg, repo.ID, manifest.Blob.Digest))
//	// verify manifest got updated
//	gotManifest := getManifest(t, pg, manifest.Blob.Digest)
//	require.WithinDuration(t, now, gotManifest.Blob.LastReferred, 100*time.Millisecond)
//	// verify refcount of referred blob did not increase
//	require.EqualValues(t, 2, gotManifest.Refers[0].Refcount)
//}
//
//// If manifest blob only (without refers) is inserted between first check and subsequent insertion by
//// another process then it is checked again it's refers and referenced blobs refcount is updated
//func TestPutNewManifestWithBlobsConcurrentlyWithoutRefers(t *testing.T) {
//	pg := newPGDB(t)
//	ctx := context.Background()
//	now := time.Now().UTC()
//	// create sample manifest
//	manifest := sampleManifest()
//	manifest.Refers[0].Refcount = 0
//	manifest.Blob.LastReferred = now.Add(-1 * time.Hour)
//	ns, name, repoName := sampleRepo()
//	// insert it in goroutine and block after initial check
//	pg.waitManifest = make(chan bool)
//	errChan := make(chan error)
//	go func() {
//		errChan <- pg.PutManifestWithBlobsAndRepo(ctx, manifest, repoName)
//	}()
//	// ensure the function waits after checking manifest exists
//	<-pg.waitManifest
//	// now insert the manifest blob only without refers simulating an insertion by blobreader
//	require.NoError(t, pg.PutBlob(ctx, &manifest.Blob))
//	// and resume the function; it should detect the insertion and update refers, refcounts and last_referred
//	<-pg.waitManifest
//	require.NoError(t, <-errChan)
//	// check repo link got created
//	repo := assertRepo(t, pg, repoName, ns, name)
//	require.True(t, isRepoLinked(pg, repo.ID, manifest.Blob.Digest))
//	// verify manifest last_referred got updated
//	gotManifest := getManifest(t, pg, manifest.Blob.Digest)
//	require.WithinDuration(t, now, gotManifest.Blob.LastReferred, 100*time.Millisecond)
//	// verify manifest refers got updated
//	require.Len(t, gotManifest.Refers, 1)
//	require.Equal(t, manifest.Refers[0].Digest, gotManifest.Refers[0].Digest)
//	// refered blobs refcount was incremented
//	require.EqualValues(t, 1, gotManifest.Refers[0].Refcount)
//}
//
//// new manifest is added on existing repo. its refcount is 0
//func TestPutNewManifestWithBlobsOnExistingRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	// create repo
//	ns, name, repoName := sampleRepo()
//	var repoId int64
//	err := pg.conn.NewSession(nil).SelectBySql(
//		"INSERT INTO repos(full_name, namespace, name) VALUES(?, ?, ?) RETURNING id",
//		repoName, ns, name).LoadOne(&repoId)
//	require.NoError(t, err)
//	// create sample manifest
//	manifest := rdb.Manifest{
//		Blob: rdb.Blob{
//			Digest: digest.Digest(uuid.Generate().String()),
//			Size:   10,
//		},
//		Refers: []*rdb.Blob{
//			{Digest: digest.Digest(uuid.Generate().String()), Size: 30},
//		},
//	}
//	// insert it
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(context.Background(), &manifest, repoName))
//	// validate its refcount is 0 and is linked to existing repo
//	gotManifest := getManifest(t, pg, manifest.Blob.Digest)
//	require.EqualValues(t, 0, gotManifest.Blob.Refcount)
//	require.WithinDuration(t, time.Now().UTC(), gotManifest.Blob.LastReferred, 100*time.Millisecond)
//	require.True(t, isRepoLinked(pg, repoId, manifest.Blob.Digest))
//}
//
//// Existing manifest's last_referred is updated and its referred blobs are not touched
//func TestPutExistingManifestWithBlobsOnNewRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	// create a manifest
//	manifest, _ := testPutNewManifestWithBlobsOnNewRepo(t, pg)
//	_, err := pg.conn.NewSession(nil).
//		Update("blobs").
//		Set("refcount", 20).
//		Where(dbr.Eq("digest", manifest.Blob.Digest)).
//		Exec()
//	require.NoError(t, err)
//	_, _, repoName := sampleRepo()
//	// insert it on diff repo
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(context.Background(), manifest, repoName))
//	// its refcount is not changed and last_referred is updated
//	gotManifest := getManifest(t, pg, manifest.Digest)
//	require.EqualValues(t, 20, gotManifest.Blob.Refcount)
//	require.NotEqual(t, manifest.Blob.LastReferred, gotManifest.Blob.LastReferred)
//	// referred blobs are not touched
//	require.EqualValues(t, manifest.Refers, gotManifest.Refers)
//}
//
//// Existing manifest that does not have `refers` is updated with refcount and `refers`
//func TestPutExistingManifestWithoutRefers(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	now := time.Now().UTC()
//	// create an incomplete manifest - one that has empty `refers`
//	manifestBlob := rdb.Blob{
//		Digest:       digest.Digest(uuid.Generate().String()),
//		Size:         10,
//		Refcount:     2,
//		LastReferred: now.Add(-1 * time.Hour),
//	}
//	require.NoError(t, pg.PutBlob(ctx, &manifestBlob))
//	// create and insert complete manifest with same digest
//	manifest := rdb.Manifest{
//		Blob: manifestBlob,
//		Refers: []*rdb.Blob{
//			{Digest: digest.Digest(uuid.Generate().String()), Size: 30},
//		},
//	}
//	_, _, repoName := sampleRepo()
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(ctx, &manifest, repoName))
//	// its refcount is not changed
//	gotManifest := getManifest(t, pg, manifest.Digest)
//	require.EqualValues(t, 2, gotManifest.Blob.Refcount)
//	// last_referred is updated
//	require.NotEqual(t, manifestBlob.LastReferred, gotManifest.Blob.LastReferred)
//	// refers is updated
//	require.Len(t, gotManifest.Refers, 1)
//	require.Equal(t, manifest.Refers[0].Digest, gotManifest.Refers[0].Digest)
//	require.EqualValues(t, 1, gotManifest.Refers[0].Refcount)
//	require.WithinDuration(t, now, gotManifest.Refers[0].LastReferred, 100*time.Millisecond)
//}
//
//// If a manifest that is already linked to a repo is inserted then its last_referred is not updated
//// Basically ensures PutManifestWithBlobsAndRepo is idempotent
//func TestPutDuplicateManifestWithBlobs(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	manifest, repo := testPutNewManifestWithBlobsOnNewRepo(t, pg)
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(context.Background(), manifest, repo.FullName))
//	require.EqualValues(t, manifest, getManifest(t, pg, manifest.Blob.Digest))
//}
//
//func getRefcount(t *testing.T, pg *pgDB, dgst digest.Digest) int64 {
//	var refcount int64
//	err := pg.conn.NewSession(nil).SelectBySql(
//		"select refcount from blobs where digest=?", dgst).LoadOne(&refcount)
//	require.NoError(t, err)
//	return refcount
//}
//
//func TestLinkTag(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	// create repo and manifest
//	manifest, repo := testPutNewManifestWithBlobsOnNewRepo(t, pg)
//	// link it
//	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", manifest))
//	// verify tag got created
//	var gotTag string
//	err := pg.conn.NewSession(nil).SelectBySql(
//		"select tag from tags where repo_id=? and tag=? and manifest=?",
//		repo.ID, "latest", manifest.Blob.Digest).LoadOne(&gotTag)
//	require.NoError(t, err)
//	// refcount increased
//	require.EqualValues(t, 1, getRefcount(t, pg, manifest.Blob.Digest))
//	// try linking again, it should not error and refcount remains same
//	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", manifest))
//	require.EqualValues(t, 1, getRefcount(t, pg, manifest.Blob.Digest))
//	// try with unknown manifest. it should error and refcount remains same
//	unknownManifest := rdb.Manifest{Blob: rdb.Blob{Digest: digest.Digest("doesnotexist")}}
//	require.NotNil(t, pg.LinkTag(ctx, repo.FullName, "latest", &unknownManifest))
//	require.EqualValues(t, 1, getRefcount(t, pg, manifest.Blob.Digest))
//	// try with unknown repo. errors and refcount remains same
//	require.NotNil(t, pg.LinkTag(ctx, "unknownrepo", "latest", manifest))
//	require.EqualValues(t, 1, getRefcount(t, pg, manifest.Blob.Digest))
//}
//
//func sampleBlob() *rdb.Blob {
//	return &rdb.Blob{
//		Digest:   digest.Digest(uuid.Generate().String()),
//		Size:     10,
//		Refcount: 2,
//	}
//}
//
//func sampleManifest() *rdb.Manifest {
//	return &rdb.Manifest{
//		Blob: rdb.Blob{
//			Digest: digest.Digest(uuid.Generate().String()),
//			Size:   10,
//		},
//		Refers: []*rdb.Blob{
//			{Digest: digest.Digest(uuid.Generate().String()), Size: 30},
//		},
//	}
//}
//
//func sampleManifest2() *rdb.Manifest {
//	return &rdb.Manifest{
//		Blob: rdb.Blob{
//			Digest: digest.Digest(uuid.Generate().String()),
//			Size:   10,
//		},
//		Refers: []*rdb.Blob{
//			{Digest: digest.Digest(uuid.Generate().String()), Size: 20},
//			{Digest: digest.Digest(uuid.Generate().String()), Size: 30},
//		},
//	}
//}
//
//func TestDeleteTag(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	// create repo, manifest and tag it
//	manifest, repo := testPutNewManifestWithBlobsOnNewRepo(t, pg)
//	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", manifest))
//	// create another manifest and tag it
//	anotherManifest := sampleManifest()
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(ctx, anotherManifest, repo.FullName))
//	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", anotherManifest))
//	// verify the manifests have increased refcount
//	require.EqualValues(t, 1, getRefcount(t, pg, manifest.Blob.Digest))
//	require.EqualValues(t, 1, getRefcount(t, pg, anotherManifest.Blob.Digest))
//
//	// delete the tag
//	require.NoError(t, pg.DeleteTag(ctx, repo.FullName, "latest"))
//	// tag is removed from db
//	var manifests []string
//	_, err := pg.conn.NewSession(nil).SelectBySql(
//		"select manifest from tags where repo_id=? and tag=?", repo.ID, "latest").Load(&manifests)
//	require.NoError(t, err)
//	require.Len(t, manifests, 0)
//	// manifests refcount is reduced
//	require.EqualValues(t, 0, getRefcount(t, pg, manifest.Blob.Digest))
//	require.EqualValues(t, 0, getRefcount(t, pg, anotherManifest.Blob.Digest))
//	// deleting again does not error
//	require.NoError(t, pg.DeleteTag(ctx, repo.FullName, "latest"))
//
//	// deleting unknown tag does not error
//	require.NoError(t, pg.DeleteTag(ctx, repo.FullName, "unknown"))
//
//	// deleting tag with unknown repo does not error
//	require.NoError(t, pg.DeleteTag(ctx, "unknownrepo", "latest"))
//
//	t.Run("inprogress repo", func(t *testing.T) {
//		// create repo, manifest and tag it
//		manifest, repo := testPutNewManifestWithBlobsOnNewRepo(t, pg)
//		require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", manifest))
//		err = pg.UpdateRepoProcessingStatus(ctx, repo.FullName, &RepoProcStatus{OverallStatus: RepoStatusInProgress})
//		require.NoError(t, err)
//		// deleting tag when repo is processing results in error
//		require.Error(t, pg.DeleteTag(ctx, repo.FullName, "latest"))
//	})
//}
//
//func TestDeleteRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	// create repo, manifest and tag it
//	manifest, repo := testPutNewManifestWithBlobsOnNewRepo(t, pg)
//	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", manifest))
//	// create another manifest and tag it
//	anotherManifest := sampleManifest()
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(ctx, anotherManifest, repo.FullName))
//	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "latest", anotherManifest))
//	// create another manifest and tag it with different name
//	newTagManifest := sampleManifest()
//	require.NoError(t, pg.PutManifestWithBlobsAndRepo(ctx, newTagManifest, repo.FullName))
//	require.NoError(t, pg.LinkTag(ctx, repo.FullName, "version", newTagManifest))
//	// verify the manifests have increased refcount
//	require.EqualValues(t, 1, getRefcount(t, pg, manifest.Blob.Digest))
//	require.EqualValues(t, 1, getRefcount(t, pg, anotherManifest.Blob.Digest))
//	require.EqualValues(t, 1, getRefcount(t, pg, newTagManifest.Blob.Digest))
//	// create repo processing status to ensure it doesnt cause deletion issues. Note that we do not
//	// need to verify if processing status got deleted because it cannot exist without repo
//	err := pg.UpdateRepoProcessingStatus(ctx, repo.FullName, &RepoProcStatus{OverallStatus: RepoStatusInProgress})
//	require.NoError(t, err)
//
//	// delete the repo
//	require.NoError(t, pg.DeleteRepo(ctx, repo.FullName))
//	// tags are removed from db
//	var manifests []string
//	_, err = pg.conn.NewSession(nil).SelectBySql(
//		"select manifest from tags where repo_id=?", repo.ID).Load(&manifests)
//	require.NoError(t, err)
//	require.Len(t, manifests, 0)
//	// manifests refcount is reduced
//	require.EqualValues(t, 0, getRefcount(t, pg, manifest.Blob.Digest))
//	require.EqualValues(t, 0, getRefcount(t, pg, anotherManifest.Blob.Digest))
//	require.EqualValues(t, 0, getRefcount(t, pg, newTagManifest.Blob.Digest))
//
//	// blob links are removed
//	var digests []string
//	_, err = pg.conn.NewSession(nil).SelectBySql(
//		"select digest from repos_blobs where repo_id=?", repo.ID).Load(&digests)
//	require.NoError(t, err)
//	require.Len(t, digests, 0)
//
//	// repo is deleted
//	var repoId int64
//	err = pg.conn.NewSession(nil).Select("full_name").From("repos").Where(dbr.Eq("id", repo.ID)).
//		LoadOne(&repoId)
//	require.EqualValues(t, err, dbr.ErrNotFound)
//
//	// deleting again does not error
//	require.NoError(t, pg.DeleteRepo(ctx, repo.FullName))
//}
//
//func sampleRepos() ([]string, []RepoRow) {
//	const num = 3
//	repos := make([]string, num)
//	rows := make([]RepoRow, num)
//	for i := 0; i < num; i++ {
//		var row RepoRow
//		row.Namespace, row.Name, row.FullName = sampleRepo()
//		repos[i] = row.FullName
//		rows[i] = row
//	}
//	return repos, rows
//}
//
//func TestAddRepos(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	// insert one of the repo to validate it is noop with that repo
//	repos, rows := sampleRepos()
//	_, err := pg.conn.NewSession(nil).
//		InsertInto("repos").
//		Columns("full_name", "namespace", "name").
//		Record(&rows[0]).
//		Exec()
//	require.NoError(t, err)
//	require.NoError(t, pg.AddRepos(ctx, repos))
//	for i := 0; i < len(rows); i++ {
//		_ = assertRepo(t, pg, rows[i].FullName, rows[i].Namespace, rows[i].Name)
//	}
//	// Try adding repo without '/'
//	repo := fmt.Sprintf("nonsrepo%s", uuid.Generate().String())
//	require.NoError(t, pg.AddRepos(ctx, []string{repo}))
//	_ = assertRepo(t, pg, repo, "", repo)
//}
//
//func TestPutBlobsAndUpsertWorker(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//
//	// grab unused workerID
//	testWorkerID := uuid.Generate().String()
//	testKey1 := "testKey1"
//	testKey2 := "testKey2"
//	testDigest1 := uuid.Generate().String()
//	testDigest2 := uuid.Generate().String()
//
//	now := time.Now().Round(time.Millisecond).UTC()
//	blob1 := rdb.Blob{
//		Digest:       digest.Digest(testDigest1),
//		Size:         20,
//		Refcount:     2,
//		LastReferred: now.Add(-1 * time.Hour),
//	}
//	blob2 := rdb.Blob{
//		Digest:       digest.Digest(testDigest2),
//		Size:         30,
//		Refcount:     1,
//		LastReferred: now.Add(-2 * time.Hour),
//	}
//
//	err := pg.PutBlobsAndUpsertWorker(ctx, []*rdb.Blob{&blob1, &blob2}, testWorkerID, testKey1)
//	require.NoError(t, err)
//
//	resBlob1 := getBlob(t, pg, blob1.Digest)
//	require.Equal(t, blob1, *resBlob1)
//
//	resBlob2 := getBlob(t, pg, blob2.Digest)
//	require.Equal(t, blob2, *resBlob2)
//
//	actualKey, err := pg.GetWorkerKey(ctx, testWorkerID)
//	require.NoError(t, err)
//
//	// repeat with same worker but different testKey and ensure worker upsert is accurate (no change to blobs since they already exist)
//	err = pg.PutBlobsAndUpsertWorker(ctx, []*rdb.Blob{&blob1, &blob2}, testWorkerID, testKey2)
//	require.NoError(t, err)
//
//	resBlob1 = getBlob(t, pg, blob1.Digest)
//	require.Equal(t, blob1, *resBlob1)
//
//	resBlob2 = getBlob(t, pg, blob2.Digest)
//	require.Equal(t, blob2, *resBlob2)
//
//	actualKey, err = pg.GetWorkerKey(ctx, testWorkerID)
//	require.NoError(t, err)
//
//	require.Equal(t, testKey2, actualKey)
//}
//
//func TestLastRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	require.NoError(t, pg.UpdateLastRepoAdded(ctx, "a/b"))
//	repo, err := pg.GetLastRepoAdded(ctx)
//	require.NoError(t, err)
//	require.Equal(t, "a/b", repo)
//	require.NoError(t, pg.UpdateLastRepoAdded(ctx, "b/d"))
//	repo, err = pg.GetLastRepoAdded(ctx)
//	require.NoError(t, err)
//	require.Equal(t, "b/d", repo)
//}
//
//func TestPutManifestOnRepo(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	t.Run("new manifest", func(t *testing.T) {
//		// create sample repo
//		repoRow := insertSampleRepo(t, pg)
//		// insert sample manifest and its referred blobs
//		manifest := sampleManifest2()
//		insertManifest(t, pg, manifest)
//		// update manifest refers to empty array to simulate blob existence
//		_, err := pg.conn.NewSession(nil).
//			Update("blobs").
//			Set("refers", "{}").
//			Where("digest=?", manifest.Blob.Digest).
//			Exec()
//		require.NoError(t, err)
//		// Call PutManifestOnRepo
//		require.NoError(t, pg.PutManifestOnRepo(ctx, manifest, repoRow.FullName))
//		// Verify that refers got updated
//		actualManifest := getManifest(t, pg, manifest.Blob.Digest)
//		require.Len(t, actualManifest.Refers, 2)
//		require.Equal(t, manifest.Refers[0].Digest, actualManifest.Refers[0].Digest)
//		require.Equal(t, manifest.Refers[1].Digest, actualManifest.Refers[1].Digest)
//		// verify that referred blobs' refcount was incremented
//		require.EqualValues(t, 1, actualManifest.Refers[0].Refcount)
//		require.EqualValues(t, 1, actualManifest.Refers[1].Refcount)
//		// verify that manifest was linked to repo
//		require.True(t, isRepoLinked(pg, repoRow.ID, manifest.Blob.Digest))
//	})
//	t.Run("existing manifest", func(t *testing.T) {
//		// create sample repo
//		repoRow := insertSampleRepo(t, pg)
//		// insert sample manifest and its referred blobs
//		manifest := sampleManifest2()
//		insertManifest(t, pg, manifest)
//		// Call PutManifestOnRepo
//		require.NoError(t, pg.PutManifestOnRepo(ctx, manifest, repoRow.FullName))
//		// Verify that nothing was updated including referred blobs' refcount
//		actualManifest := getManifest(t, pg, manifest.Blob.Digest)
//		require.EqualValues(t, manifest, actualManifest)
//		// verify that manifest was linked to repo
//		require.True(t, isRepoLinked(pg, repoRow.ID, manifest.Blob.Digest))
//	})
//	t.Run("unknown repo", func(t *testing.T) {
//		_, _, fn := sampleRepo()
//		manifest := sampleManifest2()
//		require.Error(t, pg.PutManifestOnRepo(ctx, manifest, fn))
//	})
//	t.Run("unknown manifest", func(t *testing.T) {
//		repoRow := insertSampleRepo(t, pg)
//		manifest := sampleManifest2()
//		require.Error(t, pg.PutManifestOnRepo(ctx, manifest, repoRow.FullName))
//	})
//	t.Run("unknown referred blob", func(t *testing.T) {
//		repoRow := insertSampleRepo(t, pg)
//		manifest := sampleManifest2()
//		insertManifest(t, pg, manifest)
//		// update manifest refers to empty array to simulate blob existence
//		_, err := pg.conn.NewSession(nil).
//			Update("blobs").
//			Set("refers", "{}").
//			Where("digest=?", manifest.Blob.Digest).
//			Exec()
//		require.NoError(t, err)
//		// delete one of the referred blobs
//		_, err = pg.conn.NewSession(nil).
//			DeleteFrom("blobs").
//			Where("digest=?", manifest.Refers[1].Digest).
//			Exec()
//		require.NoError(t, err, err)
//		require.Error(t, pg.PutManifestOnRepo(ctx, manifest, repoRow.FullName))
//		// validate manifest was not updated
//		actualManifest := getManifest(t, pg, manifest.Blob.Digest)
//		require.Len(t, actualManifest.Refers, 0)
//	})
//}
//
//func TestLinkBlobs(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	// sample repo
//	repo := insertSampleRepo(t, pg)
//	// insert sample blobs
//	blobs := []*rdb.Blob{sampleBlob(), sampleBlob(), sampleBlob()}
//	for _, blob := range blobs {
//		require.NoError(t, pg.PutBlob(ctx, blob))
//	}
//	// link one of the blobs to repo to check upsert
//	_, err := pg.conn.NewSession(nil).
//		InsertInto("repos_blobs").
//		Columns("repo_id", "digest").
//		Values(repo.ID, blobs[1].Digest).
//		Exec()
//	require.NoError(t, err)
//	// call func
//	require.NoError(t, pg.LinkBlobs(ctx, blobDigests(blobs), repo.FullName))
//	// validate blobs got linked
//	for _, blob := range blobs {
//		require.True(t, isRepoLinked(pg, repo.ID, blob.Digest))
//	}
//}
//
//// blobDigests returns slics of digests in blobs. Note that this is different from `digests` which
//// returns digests as slice of strings
//func blobDigests(blobs []*rdb.Blob) []digest.Digest {
//	dgsts := make([]digest.Digest, len(blobs))
//	for i, blob := range blobs {
//		dgsts[i] = blob.Digest
//	}
//	return dgsts
//}
//
//func TestLinkTagManifests(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	// create sample repo
//	repo := insertSampleRepo(t, pg)
//	// create sample manifests; they can be blobs for the purpose of this test since db schema does
//	// not distinguish between manifest and regular blobs
//	blobs := []*rdb.Blob{sampleBlob(), sampleBlob(), sampleBlob()}
//	for _, blob := range blobs {
//		require.NoError(t, pg.PutBlob(ctx, blob))
//	}
//	// link one of the blobs to the tag to test for existing link case (blob refcount should not increase)
//	tag := "randtag"
//	_, err := pg.conn.NewSession(nil).
//		InsertInto("tags").
//		Columns("repo_id", "tag", "manifest").
//		Values(repo.ID, tag, blobs[2].Digest).
//		Exec()
//	require.NoError(t, err)
//	// call func
//	require.NoError(t, pg.LinkTagManifests(ctx, repo.FullName, tag, blobDigests(blobs)))
//	// verify that manifests were linked to the tag
//	for _, blob := range blobs {
//		var gotTag string
//		err := pg.conn.NewSession(nil).SelectBySql(
//			"select tag from tags where repo_id=? and tag=? and manifest=?",
//			repo.ID, tag, blob.Digest).LoadOne(&gotTag)
//		require.NoError(t, err)
//	}
//	// verify refcount was increased for newly linked manifests
//	for i := 0; i < 2; i++ {
//		gotBlob := getBlob(t, pg, blobs[i].Digest)
//		require.EqualValues(t, blobs[i].Refcount+1, gotBlob.Refcount)
//	}
//	// verify refcount was NOT increased for already linked manifest
//	require.EqualValues(t, blobs[2].Refcount, getBlob(t, pg, blobs[2].Digest).Refcount)
//}
//
//func TestEnumerateIncompleteRepos(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	// insert sample repos
//	repos := make([]*RepoRow, 5)
//	for i := 0; i < len(repos); i++ {
//		repos[i] = insertSampleRepo(t, pg)
//	}
//	// add some statuses for these repos
//	addStatus := func(repo *RepoRow, status RepoCompletionStatus) {
//		_, err := pg.conn.NewSession(nil).
//			InsertInto("repoloading_status").
//			Columns("repo_id", "status").
//			Values(repo.ID, status).
//			Exec()
//		require.NoError(t, err)
//	}
//	addStatus(repos[0], RepoStatusComplete)
//	addStatus(repos[1], RepoStatusError)
//	addStatus(repos[2], RepoStatusInProgress)
//	// get incomplete repos
//	actualRepos := make(map[string]struct{})
//	err := pg.EnumerateIncompleteRepos(ctx, func(repo string) {
//		actualRepos[repo] = struct{}{}
//	})
//	require.NoError(t, err)
//	// Since this test is running along with other tests we will most likely get other repos. However,
//	// we will just ensure that it contains the repos we are interested in
//	require.True(t, len(actualRepos) >= 4)
//	for i := 1; i < len(repos); i++ {
//		require.Contains(t, actualRepos, repos[i].FullName)
//	}
//}
//
//func TestRepoProcessingStatus(t *testing.T) {
//	t.Parallel()
//	pg := getPGDB(t)
//	ctx := context.Background()
//	t.Run("non-existent-status", func(t *testing.T) {
//		repo := insertSampleRepo(t, pg)
//		status, err := pg.GetRepoProcessingStatus(ctx, repo.FullName)
//		require.NoError(t, err)
//		require.EqualValues(t, status, &RepoProcStatus{OverallStatus: RepoStatusNotStarted})
//	})
//	t.Run("insert-get-update-status", func(t *testing.T) {
//		repo := insertSampleRepo(t, pg)
//		// insert status
//		status := &RepoProcStatus{OverallStatus: RepoStatusInProgress, BlobsCompleted: true}
//		require.NoError(t, pg.UpdateRepoProcessingStatus(ctx, repo.FullName, status))
//		// get and validate it is same
//		actualStatus, err := pg.GetRepoProcessingStatus(ctx, repo.FullName)
//		require.NoError(t, err)
//		require.EqualValues(t, status, actualStatus)
//		// update and get again and see
//		status.OverallStatus = RepoStatusComplete
//		status.ManifestsCompleted = true
//		require.NoError(t, pg.UpdateRepoProcessingStatus(ctx, repo.FullName, status))
//		actualStatus, err = pg.GetRepoProcessingStatus(ctx, repo.FullName)
//		require.NoError(t, err)
//		require.EqualValues(t, status, actualStatus)
//	})
//}
