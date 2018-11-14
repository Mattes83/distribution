-- Represents any blob stored in registry. Can be manifest, layer or config json
CREATE TABLE blobs(
	digest text PRIMARY KEY,
	-- number of entities referring to this blob. Will be eventually GCed if value is 0
	refcount bigint NOT NULL,
	size bigint NOT NULL,
	last_referred timestamp without time zone NOT NULL,
	-- digests of blobs that this blob refers to
	refers text[] DEFAULT '{}'
);

CREATE TABLE repos(
	id bigserial PRIMARY KEY,
	-- namespace/name format
	full_name text UNIQUE NOT NULL,
	-- separated `full_name` 
	-- these are technically not required but are good to have
	namespace text NOT NULL,
	name text NOT NULL
);

-- store tag references to manifests
CREATE TABLE tags(
	repo_id bigint REFERENCES repos(id),
	tag text,
	manifest text REFERENCES blobs(digest),
	PRIMARY KEY (repo_id, tag)
);

-- registry stores manifest and layer links in repos. This table represents that
-- It will be useful to find untagged images and also remove these references when GCing
CREATE TABLE repos_blobs(
	repo_id bigint REFERENCES repos(id),
	digest text  REFERENCES blobs(digest),
	PRIMARY KEY(repo_id, digest)
);
