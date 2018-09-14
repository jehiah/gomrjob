// simple functions for interacting with Google Storage over the JSON API
//
// This provides a no-dependency interaction with Google Storage
// https://cloud.google.com/storage/docs/json_api
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
)

const APIBase = "https://www.googleapis.com"

// Insert using the "Simple Media" API
// https://cloud.google.com/storage/docs/json_api/v1/how-tos/simple-upload
func Insert(ctx context.Context, c *http.Client, bucket, name, contentType string, body io.Reader) error {
	params := url.Values{"name": []string{name}}
	endpoint := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", APIBase, url.PathEscape(bucket), params.Encode())
	req, err := http.NewRequest("POST", endpoint, body)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("got status code %d on upload of gs://%s/%s", resp.StatusCode, bucket, name)
	}
	return nil
}

// Object represents a single item in GS
// https://cloud.google.com/storage/docs/json_api/v1/objects#resource
type Object struct {
	ID           string `json:"id"`
	Bucket       string `json:"bucket"`
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	Created      string `json:"timeCreated"`
	StorageClass string `json:"storageClass"`
	MD5Hash      string `json:"md5Hash"`
	Updateed     string `json:"updated"`
}

type listResp struct {
	King          string   `json:"kind"` // storage#objects
	NextPageToken string   `json:"nextPageToken"`
	Prefixes      []string `json:"prefixes"`
	Items         []Object `json:"items"`
}

// List returns up to 1k items matching a prefix
// https://cloud.google.com/storage/docs/json_api/v1/objects/list
// GET https://www.googleapis.com/storage/v1/b/bucket/o?prefix=....
func List(ctx context.Context, c *http.Client, bucket, prefix, token string) (items []Object, pageToken string, err error) {
	params := url.Values{"prefix": []string{prefix}}
	if token != "" {
		params.Set("pageToken", token)
	}
	endpoint := fmt.Sprintf("%s/storage/v1/b/%s/o?%s", APIBase, url.PathEscape(bucket), params.Encode())
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, "", err
	}
	req = req.WithContext(ctx)
	resp, err := c.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, "", fmt.Errorf("got status code %d on list of gs://%s/%s", resp.StatusCode, bucket, prefix)
	}
	var o listResp
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
		return nil, "", err
	}
	return o.Items, o.NextPageToken, nil
}

// Delete
// https://cloud.google.com/storage/docs/json_api/v1/objects/delete?authuser=1
func Delete(ctx context.Context, c *http.Client, bucket, name string) error {
	log.Printf("deleting gs://%s/%s", bucket, name)
	endpoint := fmt.Sprintf("%s/storage/v1/b/%s/o/%s", APIBase, url.PathEscape(bucket), url.PathEscape(name))
	req, err := http.NewRequest("DELETE", endpoint, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("got status code %d on delete of gs://%s/%s", resp.StatusCode, bucket, name)
	}
	return nil
}

func DeletePrefix(ctx context.Context, c *http.Client, bucket, prefix string) error {
	items, token, err := List(ctx, c, bucket, prefix, "")
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	for token != "" {
		for _, obj := range items {
			if err := Delete(ctx, c, bucket, obj.Name); err != nil {
				return err
			}
		}
		items, token, err = List(ctx, c, bucket, prefix, token)
		if err != nil {
			return err
		}
	}
	return nil
}
