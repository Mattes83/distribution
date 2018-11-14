package pg

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/docker/distribution/registry/storage/rdb"
	"github.com/gocraft/dbr"
	"github.com/lib/pq"
	_ "github.com/mattes/migrate/driver/postgres" // Load the postgres schema migration driver
	"github.com/mattes/migrate/migrate"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

// Postgres implementation of RepoMetadataDB

type pgDB struct {
	conn         *dbr.Connection
	dsn          string
	waitRepo     chan bool
	waitManifest chan bool
}

func NewDB(ctx context.Context, dsn string, migrationsDir string) (rdb.RepoMetadataDB, error) {
	// create db connection
	conn, err := dbr.Open("postgres", dsn, nil)
	if err != nil {
		return nil, err
	}

	// Ping the DB to ensure connection is valid
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = conn.PingContext(ctx); err != nil {
		return nil, err
	}

	// updrade schema
	errs, ok := migrate.UpSync(dsn, migrationsDir)
	if !ok {
		return nil, fmt.Errorf("Error upgrading schema: %v", errs)
	}

	// set other params and return
	conn.SetMaxOpenConns(10)
	return &pgDB{
		conn: conn,
		dsn:  dsn,
	}, nil
}

// digests returns string slice of digests in given blobs
func digests(blobs []*rdb.Blob) []string {
	dgsts := make([]string, len(blobs))
	for i, blob := range blobs {
		dgsts[i] = string(blob.Digest)
	}
	return dgsts
}

// starts a transaction with given fields and executes given function within that transaction
func (pg *pgDB) withTransaction(ctx context.Context, do func(tx *dbr.Tx) error) error {
	sess := pg.conn.NewSession(nil)
	tx, err := sess.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "Error starting transaction")
	}
	defer tx.RollbackUnlessCommitted()
	err = do(tx)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "Error committing transaction")
	}
	return nil
}

// add repo if it doesnt exist
func (pg *pgDB) ensureRepoAdded(tx *dbr.Tx, ctx context.Context, repo string) (repoId int64, err error) {
	var getRepoId = func() (int64, error) {
		var id int64
		err := tx.Select("id").
			From("repos").
			Where(dbr.Eq("full_name", repo)).
			LoadOneContext(ctx, &id)
		return id, err
	}
	// try getting repo id assuming its already there. This should be most common case
	repoId, err = getRepoId()
	if err == nil {
		return repoId, nil
	}
	if err != dbr.ErrNotFound {
		return 0, errors.Wrap(err, "Error getting repo id")
	}
	// block on waitRepo channel if it exists (for testing)
	if pg.waitRepo != nil {
		// first wait is to allow the test to know that we've stopped
		pg.waitRepo <- true
		// second wait is for test to resume execution after doing any operaiton (like creating repo)
		pg.waitRepo <- true
	}
	// Repo does not exist; create one
	splits := strings.SplitN(repo, "/", 2)
	if len(splits) != 2 {
		return 0, fmt.Errorf("repo name '%s' does not have /", repo)
	}
	namespace, name := splits[0], splits[1]
	err = tx.SelectBySql(
		`INSERT INTO repos(full_name, namespace, name)
		 VALUES(?, ?, ?)
		 ON CONFLICT DO NOTHING
		 RETURNING id`,
		repo, namespace, name).LoadOneContext(ctx, &repoId)
	if err != nil {
		if err == dbr.ErrNotFound {
			// Looks like repo got inserted between first check and this insertion by another process.
			// Get it again
			return getRepoId()
		}
		return 0, errors.Wrap(err, "error inserting repo")
	}
	return repoId, nil
}

func ensureRepoLinked(tx *dbr.Tx, ctx context.Context, repoId int64, dgst digest.Digest) (bool, error) {
	r, err := tx.InsertBySql(
		"INSERT INTO repos_blobs(repo_id, digest) VALUES (?, ?) ON CONFLICT(repo_id, digest) DO NOTHING",
		repoId, dgst).ExecContext(ctx)
	if err != nil {
		return false, errors.Wrap(err, "error linking repo to blob/manifest")
	}
	numRows, err := r.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "error getting rows when linking blob")
	}
	return numRows == 0, nil
}

func (pg *pgDB) ensureRepoAddedLinked(tx *dbr.Tx, ctx context.Context, blob *rdb.Blob, repo string) (bool, error) {
	// add repo
	repoId, err := pg.ensureRepoAdded(tx, ctx, repo)
	if err != nil {
		return false, err
	}

	// insert link in repos_blobs only if it doesnt exist
	return ensureRepoLinked(tx, ctx, repoId, blob.Digest)
}

func (pg *pgDB) PutBlobWithRepo(ctx context.Context, blob *rdb.Blob, repo string) error {
	sess := pg.conn.NewSession(nil)
	tx, err := sess.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "couldn't start transaction")
	}
	defer tx.RollbackUnlessCommitted()

	// add blob
	now := time.Now().UTC()
	_, err = tx.InsertBySql(
		`INSERT INTO blobs(digest, refcount, size, last_referred) 
		 VALUES(?, 0, ?, ?) 
		 ON CONFLICT (digest) DO UPDATE SET last_referred=?`,
		blob.Digest, blob.Size, now, now).ExecContext(ctx)
	if err != nil {
		return errors.Wrap(err, "error upserting blob")
	}

	// add repo and link it
	blobAlreadyLinked, err := pg.ensureRepoAddedLinked(tx, ctx, blob, repo)
	if err != nil {
		return err
	}
	if blobAlreadyLinked {
		// The blob is already linked. Rollback to avoid updating refcount
		err = tx.Rollback()
		if err != nil {
			return errors.Wrap(err, "error rolling back transaction")
		}
		return nil
	}
	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "error committing transaction")
	}
	return nil
}

func getRepoId(ctx context.Context, sess *dbr.Session, repoName string) (int64, error) {
	// TODO: Add in-memory LRU cache
	var repoId int64
	err := sess.Select("id").
		From("repos").
		Where(dbr.Eq("full_name", repoName)).
		LoadOneContext(ctx, &repoId)
	if err != nil {
		return 0, errors.Wrap(err, "getrepoid failed")
	}
	return repoId, nil
}

func (pg *pgDB) upsertManifest(ctx context.Context, manifest *rdb.Manifest) error {
	return pg.withTransaction(ctx, func(tx *dbr.Tx) error {
	start:
		now := time.Now().UTC()
		var count int
		err := tx.SelectBySql(
			"SELECT COALESCE(array_length(refers, 1), 0) FROM blobs WHERE digest=? FOR UPDATE", manifest.Blob.Digest).
			LoadOneContext(ctx, &count)
		if err != nil && err != dbr.ErrNotFound {
			return errors.Wrap(err, "getManifestRefersLen")
		}
		// manifest info already exists. Only update last_referred
		if count > 0 {
			_, err := tx.Update("blobs").
				Set("last_referred", now).
				Where(dbr.Eq("digest", manifest.Blob.Digest)).
				ExecContext(ctx)
			if err != nil {
				return errors.Wrap(err, "updateManifestLastReferred")
			}
			return nil
		}

		// block on wait channel if it exists (for testing)
		if pg.waitManifest != nil {
			// first wait is to allow the test to know that we've stopped
			pg.waitManifest <- true
			// second wait is for test to resume execution after doing any operaiton (like creating manifest)
			pg.waitManifest <- true
			// reset to nil so that if controls move back to "start" label again then we would like to
			// have normal flow without blocking
			pg.waitManifest = nil
		}

		refersDigests := digests(manifest.Refers)
		if err == dbr.ErrNotFound {
			// manifest doesn't exist; create new
			r, err := tx.InsertBySql(
				`INSERT INTO blobs(digest, refcount, size, last_referred, refers, payload)
				 VALUES(?, ?, ?, ?, ?, ?)
				 ON CONFLICT(digest) DO NOTHING`,
				manifest.Blob.Digest, 0, manifest.Blob.Size, now, pq.Array(refersDigests), manifest.Payload).
				ExecContext(ctx)
			if err != nil {
				return errors.Wrap(err, "manifestinsert")
			}
			numRows, err := r.RowsAffected()
			if err != nil {
				return errors.Wrap(err, "insertManifest")
			}
			if numRows == 0 {
				// looks like another process inserted between above check and this insertion. restart
				goto start
			}
		} else {
			// manifest exists; update refers and last_referred
			_, err := tx.Update("blobs").
				Set("size", manifest.Blob.Size).
				Set("last_referred", now).
				Set("refers", pq.Array(refersDigests)).
				Set("payload", manifest.Payload).
				Where(dbr.Eq("digest", manifest.Blob.Digest)).
				ExecContext(ctx)
			if err != nil {
				return errors.Wrap(err, "updateManifestRefers")
			}
		}

		// update referred blob's refcount
		// sort "refers" alphabetically to ensure there is no deadlock when two trans are trying to
		// insert manifest with common blobs
		refers := make([]*rdb.Blob, len(manifest.Refers))
		_ = copy(refers, manifest.Refers)
		// sorting a copy to avoid mutating caller's argument
		sort.Slice(refers, func(i, j int) bool {
			return refers[i].Digest < refers[j].Digest
		})
		for _, blob := range refers {
			r, err := tx.UpdateBySql(
				`UPDATE blobs SET refcount=refcount+1, last_referred=now() WHERE digest=?`,
				blob.Digest).ExecContext(ctx)
			if err != nil {
				return errors.Wrap(err, "updateBlobRefcount")
			}
			ra, err := r.RowsAffected()
			if err != nil {
				return errors.Wrap(err, "getRowsAffected")
			}
			if ra != 1 {
				return rdb.BlobNotFoundErr{Digest: blob.Digest}
			}
		}
		return nil
	})
}

func (pg *pgDB) PutManifestOnRepo(ctx context.Context, manifest *rdb.Manifest, repo string) error {
	sess := pg.conn.NewSession(nil)

	// get repo id
	repoId, err := getRepoId(ctx, sess, repo)
	if err != nil {
		return err
	}

	// check if manifest is already linked
	var dgst string
	err = sess.Select("digest").
		From("repos_blobs").
		Where(dbr.And(dbr.Eq("repo_id", repoId), dbr.Eq("digest", manifest.Blob.Digest))).
		LoadOneContext(ctx, &dgst)
	if err != nil && err != dbr.ErrNotFound {
		return errors.Wrap(err, "getrepolink failed")
	}
	if dgst != "" {
		// manifest already linked. return
		return nil
	}

	// update manifest with blobs info if not already done
	err = pg.upsertManifest(ctx, manifest)
	if err != nil {
		return err
	}

	// link the blob
	_, err = sess.InsertBySql(
		"INSERT INTO repos_blobs(repo_id, digest) VALUES (?, ?) ON CONFLICT(repo_id, digest) DO NOTHING",
		repoId, manifest.Blob.Digest).ExecContext(ctx)
	if err != nil {
		return errors.Wrap(err, "insertrepolink failed")
	}

	return nil
}

func (pg *pgDB) GetRepoManifest(ctx context.Context, repo string, dgst digest.Digest) (*rdb.Manifest, error) {
	sess := pg.conn.NewSession(nil)
	// NOTE: Not getting refers as it is not used by caller
	query := `
		SELECT 
			b.digest, 
			b.size, 
			b.payload 
		FROM 
			blobs b 
			JOIN repos_blobs rb ON rb.digest = b.digest 
			JOIN repos r ON r.id = rb.repo_id
	  WHERE 
	    r.full_name = ? 
	    AND b.digest = ?`
	var manifest rdb.Manifest
	err := sess.SelectBySql(query, repo, string(dgst)).LoadOneContext(ctx, &manifest)
	if err != nil {
		return nil, errors.Wrap(err, "error getting repo manifest")
	}
	return &manifest, nil
}

func (pg *pgDB) LinkTag(ctx context.Context, repo string, tag string, manifest digest.Digest) error {
	sess := pg.conn.NewSession(nil)
	repoId, err := getRepoId(ctx, sess, repo)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO tags 
		VALUES 
			(?, ?, ?) ON CONFLICT(repo_id, tag) DO 
		UPDATE 
		SET 
			manifest = EXCLUDED.manifest`
	_, err = sess.InsertBySql(query, repoId, tag, manifest).ExecContext(ctx)
	if err != nil {
		return errors.Wrap(err, "error linking tag")
	}
	return nil
}

func (pg *pgDB) GetTagManifest(ctx context.Context, repo string, tag string) (*rdb.Manifest, error) {
	sess := pg.conn.NewSession(nil)
	// NOTE: Not getting refers as it is not used by caller
	query := `
		SELECT 
			b.digest, 
			b.size, 
			b.payload 
		FROM 
			blobs b 
			JOIN tags t ON t.manifest = b.digest 
			JOIN repos r ON r.id = rb.repo_id
	  WHERE 
	    r.full_name = ? 
	    AND t.tag = ?`
	var manifest rdb.Manifest
	err := sess.SelectBySql(query, repo, tag).LoadOneContext(ctx, &manifest.Blob)
	if err != nil {
		return nil, errors.Wrap(err, "error getting repo manifest")
	}
	return &manifest, nil
}

func (p *pgDB) DeleteTag(ctx context.Context, repo string, tag string) error {
	panic("not implemented")
}

func (p *pgDB) DeleteRepo(ctx context.Context, repo string) error {
	panic("not implemented")
}

func (pg *pgDB) IsManifestLinked(ctx context.Context, repo string, dgst digest.Digest) (bool, error) {
	// TODO: Temp delegating to blob link. Will implement later
	return pg.IsBlobLinked(ctx, repo, dgst)
}

func (pg *pgDB) IsBlobLinked(ctx context.Context, repo string, dgst digest.Digest) (bool, error) {
	sess := pg.conn.NewSession(nil)
	// get repo id
	repoId, err := getRepoId(ctx, sess, repo)
	if err != nil {
		return false, err
	}

	// check if manifest is linked
	var dgstDB string
	err = sess.Select("digest").
		From("repos_blobs").
		Where(dbr.And(dbr.Eq("repo_id", repoId), dbr.Eq("digest", dgst))).
		LoadOneContext(ctx, &dgstDB)
	if err == dbr.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "getrepolink failed")
	}
	return true, nil
}

func (pg *pgDB) PutBlob(ctx context.Context, blob *rdb.Blob) error {
	sess := pg.conn.NewSession(nil)
	_, err := sess.InsertBySql(
		"INSERT INTO blobs(digest, refcount, size, last_referred) VALUES(?, ?, ?, ?) "+
			"ON CONFLICT(digest) DO NOTHING",
		blob.Digest, blob.Refcount, blob.Size, blob.LastReferred).ExecContext(ctx)
	if err != nil {
		return errors.Wrap(err, "Error upserting blob")
	}
	return nil
}
