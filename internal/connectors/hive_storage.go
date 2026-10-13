/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package connectors

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/colinmarc/hdfs/v2"
)

type hiveStorageConfig struct {
	Endpoint       string
	Region         string
	AccessKey      string
	SecretKey      string
	ForcePathStyle *bool
	HDFSUser       string
}

type hiveFileSystem interface {
	ListParquet(ctx context.Context, location string) ([]string, error)
	Read(ctx context.Context, location string) ([]byte, error)
	Write(ctx context.Context, location string, data []byte) error
	Close() error
}

type hiveLocation struct {
	Scheme string
	Bucket string
	Host   string
	Path   string
	Raw    string
}

func parseHiveLocation(raw string) (hiveLocation, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return hiveLocation{}, fmt.Errorf("invalid Hive location %q", raw)
	}
	loc := hiveLocation{Scheme: strings.ToLower(u.Scheme), Raw: raw}
	switch loc.Scheme {
	case "s3", "s3a", "s3n":
		if u.Host == "" {
			return hiveLocation{}, fmt.Errorf("S3 location %q requires a bucket", raw)
		}
		loc.Bucket = u.Host
		loc.Path = strings.TrimPrefix(u.Path, "/")
	case "hdfs":
		if u.Port() == "" {
			return hiveLocation{}, fmt.Errorf("HDFS HA nameservice is not supported; use hdfs://host:port/path")
		}
		loc.Host = u.Host
		loc.Path = u.Path
		if loc.Path == "" {
			loc.Path = "/"
		}
	case "file":
		loc.Path = u.Path
		if loc.Path == "" {
			return hiveLocation{}, fmt.Errorf("file location %q requires a path", raw)
		}
	default:
		return hiveLocation{}, fmt.Errorf("unsupported Hive location scheme %q", loc.Scheme)
	}
	return loc, nil
}

func (l hiveLocation) child(name string) (string, error) {
	switch l.Scheme {
	case "file":
		p := filepath.Join(l.Path, name)
		return (&url.URL{Scheme: "file", Path: p}).String(), nil
	case "hdfs":
		p := path.Join(l.Path, name)
		return "hdfs://" + l.Host + p, nil
	case "s3", "s3a", "s3n":
		key := strings.Trim(l.Path, "/")
		if key != "" {
			key += "/"
		}
		key += name
		return l.Scheme + "://" + l.Bucket + "/" + key, nil
	default:
		return "", fmt.Errorf("unsupported Hive location scheme %q", l.Scheme)
	}
}

type remoteHiveFS struct {
	cfg  hiveStorageConfig
	mu   sync.Mutex
	s3c  *s3.Client
	hdfs map[string]*hdfs.Client
}

func newRemoteHiveFS(cfg hiveStorageConfig) hiveFileSystem {
	return &remoteHiveFS{cfg: cfg, hdfs: map[string]*hdfs.Client{}}
}

func (f *remoteHiveFS) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var first error
	for _, c := range f.hdfs {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	f.hdfs = map[string]*hdfs.Client{}
	return first
}

func (f *remoteHiveFS) ListParquet(ctx context.Context, location string) ([]string, error) {
	loc, err := parseHiveLocation(location)
	if err != nil {
		return nil, err
	}
	switch loc.Scheme {
	case "file":
		return listLocalParquet(loc.Path)
	case "hdfs":
		client, err := f.hdfsClient(loc.Host)
		if err != nil {
			return nil, err
		}
		return listHDFSParquet(client, loc.Host, loc.Path)
	default:
		client, err := f.s3Client(ctx)
		if err != nil {
			return nil, err
		}
		return listS3Parquet(ctx, client, loc)
	}
}

func (f *remoteHiveFS) Read(ctx context.Context, location string) ([]byte, error) {
	loc, err := parseHiveLocation(location)
	if err != nil {
		return nil, err
	}
	switch loc.Scheme {
	case "file":
		return os.ReadFile(loc.Path)
	case "hdfs":
		client, err := f.hdfsClient(loc.Host)
		if err != nil {
			return nil, err
		}
		r, err := client.Open(loc.Path)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	default:
		client, err := f.s3Client(ctx)
		if err != nil {
			return nil, err
		}
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(loc.Bucket),
			Key:    aws.String(loc.Path),
		})
		if err != nil {
			return nil, err
		}
		defer out.Body.Close()
		return io.ReadAll(out.Body)
	}
}

func (f *remoteHiveFS) Write(ctx context.Context, location string, data []byte) error {
	loc, err := parseHiveLocation(location)
	if err != nil {
		return err
	}
	switch loc.Scheme {
	case "file":
		if err := os.MkdirAll(filepath.Dir(loc.Path), 0o755); err != nil {
			return err
		}
		return os.WriteFile(loc.Path, data, 0o644)
	case "hdfs":
		client, err := f.hdfsClient(loc.Host)
		if err != nil {
			return err
		}
		if err := client.MkdirAll(path.Dir(loc.Path), 0o755); err != nil {
			return err
		}
		_ = client.Remove(loc.Path)
		w, err := client.Create(loc.Path)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			_ = w.Close()
			return err
		}
		return w.Close()
	default:
		client, err := f.s3Client(ctx)
		if err != nil {
			return err
		}
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(loc.Bucket),
			Key:    aws.String(loc.Path),
			Body:   bytes.NewReader(data),
		})
		return err
	}
}

func (f *remoteHiveFS) s3Client(ctx context.Context) (*s3.Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.s3c != nil {
		return f.s3c, nil
	}
	region := f.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(region))
	if f.cfg.AccessKey != "" || f.cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(f.cfg.AccessKey, f.cfg.SecretKey, "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("S3 config: %w", err)
	}
	pathStyle := f.cfg.Endpoint != ""
	if f.cfg.ForcePathStyle != nil {
		pathStyle = *f.cfg.ForcePathStyle
	}
	f.s3c = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = pathStyle
		if f.cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(f.cfg.Endpoint)
		}
	})
	return f.s3c, nil
}

func (f *remoteHiveFS) hdfsClient(hostport string) (*hdfs.Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.hdfs[hostport]; ok {
		return c, nil
	}
	user := f.cfg.HDFSUser
	if user == "" {
		user = "dataflow"
	}
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{hostport},
		User:      user,
	})
	if err != nil {
		return nil, fmt.Errorf("HDFS client %s: %w", hostport, err)
	}
	f.hdfs[hostport] = client
	return client, nil
}

func listLocalParquet(root string) ([]string, error) {
	info, err := os.Stat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		if isParquetName(info.Name()) {
			return []string{(&url.URL{Scheme: "file", Path: root}).String()}, nil
		}
		return nil, nil
	}
	var out []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		name := d.Name()
		if name != "." && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
			if d.IsDir() && p != root {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() || !isParquetName(name) {
			return nil
		}
		out = append(out, (&url.URL{Scheme: "file", Path: p}).String())
		return nil
	})
	return out, err
}

func listHDFSParquet(client *hdfs.Client, host, root string) ([]string, error) {
	var out []string
	var walk func(string) error
	walk = func(dir string) error {
		entries, err := client.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for _, ent := range entries {
			name := ent.Name()
			if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				continue
			}
			child := path.Join(dir, name)
			if ent.IsDir() {
				if err := walk(child); err != nil {
					return err
				}
				continue
			}
			if isParquetName(name) {
				out = append(out, "hdfs://"+host+child)
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, err
	}
	return out, nil
}

func listS3Parquet(ctx context.Context, client *s3.Client, loc hiveLocation) ([]string, error) {
	prefix := loc.Path
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var token *string
	var out []string
	for {
		page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(loc.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			key := *obj.Key
			base := path.Base(key)
			if strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_") || !isParquetName(base) {
				continue
			}
			out = append(out, loc.Scheme+"://"+loc.Bucket+"/"+key)
		}
		if page.IsTruncated == nil || !*page.IsTruncated || page.NextContinuationToken == nil {
			break
		}
		token = page.NextContinuationToken
	}
	return out, nil
}

func isParquetName(name string) bool {
	return strings.HasSuffix(strings.ToLower(name), ".parquet")
}
