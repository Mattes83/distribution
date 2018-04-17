// Package middleware - returns bittorrent file w.r.t content

package middleware

import (
	"context"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/middleware"
)

// torrentStorageMiddleware provides a torrent file for any blob
type torrentStorageMiddleware struct {
	storagedriver.StorageDriver
	peerCache     map[string][]string
	peerAvaialble time.Duration
}

var _ storagedriver.StorageDriver = &torrentStorageMiddleware{}

func newTorrentStorageMiddleware(storageDriver storagedriver.StorageDriver, options map[string]interface{}) (storagedriver.StorageDriver, error) {

	return &torrentStorageMiddleware{
		StorageDriver: storageDriver,
		peerCache:     make(map[string][]string),
		peerAvaialble: 4 * time.Hours(),
	}, nil
}

// S3BucketKeyer is any type that is capable of returning the S3 bucket key
// which should be cached by AWS CloudFront.
type S3BucketKeyer interface {
	S3BucketKey(path string) string
}

// GetContent will return torrent file as response if client advertises its support
func (lh *torrentStorageMiddleware) GetContent(ctx context.Context, path string) ([]byte, error) {
	// Return torrent file if client supports bittorrent
}

// init registers the cloudfront layerHandler backend.
func init() {
	storagemiddleware.Register("torrent", storagemiddleware.InitFunc(newTorrentStorageMiddleware))
}
