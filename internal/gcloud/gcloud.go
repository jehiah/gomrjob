package gcloud

import (
	"context"
	"net/http"
	"os"

	"golang.org/x/oauth2/google"
)

const (
	ScopeCloudPlatform string = "https://www.googleapis.com/auth/cloud-platform"
	// https://cloud.google.com/storage/docs/json_api/v1/how-tos/authorizing
	ScopeStorageReadWrite string = "https://www.googleapis.com/auth/devstorage.read_write"
)

// This reads a json service account credential file, and returns a http.Client that's bound to an
// oauth2 access token for the provided scopes
func LoadFromServiceJSON(serviceAccountPath string, scope ...string) (*http.Client, error) {
	data, err := os.ReadFile(serviceAccountPath)
	if err != nil {
		return nil, err
	}
	conf, err := google.JWTConfigFromJSON(data, scope...)
	if err != nil {
		return nil, err
	}
	return conf.Client(context.Background()), nil
}
