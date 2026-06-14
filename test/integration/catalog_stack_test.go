//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	minioAccessKey = "admin"
	minioSecretKey = "password"
	minioRegion    = "us-east-1"
	minioBucket    = "warehouse"
)

// objectStorageStack provides MinIO and AWS env vars for Iceberg/Nessie catalog sinks.
type objectStorageStack struct {
	network       *testcontainers.DockerNetwork
	minio         testcontainers.Container
	minioEndpoint string
	cleanup       func()
}

func startObjectStorageStack(ctx context.Context, t *testing.T) *objectStorageStack {
	t.Helper()
	skipUnlessDocker(t)

	net, err := network.New(ctx)
	requireDocker(t, err)

	minioCtr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          "minio/minio:RELEASE.2024-06-28T09-06-49Z",
			ExposedPorts:   []string{"9000/tcp"},
			Networks:       []string{net.Name},
			NetworkAliases: map[string][]string{net.Name: {"minio"}},
			Env: map[string]string{
				"MINIO_ROOT_USER":     minioAccessKey,
				"MINIO_ROOT_PASSWORD": minioSecretKey,
			},
			Cmd: []string{"server", "/data"},
			WaitingFor: wait.ForHTTP("/minio/health/ready").
				WithPort("9000/tcp").
				WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	requireDocker(t, err)

	minioHost, err := minioCtr.Host(ctx)
	require.NoError(t, err)
	minioPort, err := minioCtr.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	minioEndpoint := fmt.Sprintf("http://%s:%s", minioHost, minioPort.Port())

	requireDocker(t, ensureMinioBucket(ctx, net.Name, minioBucket))

	t.Setenv("AWS_ACCESS_KEY_ID", minioAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioSecretKey)
	t.Setenv("AWS_REGION", minioRegion)
	t.Setenv("AWS_S3_ENDPOINT", minioEndpoint)

	stack := &objectStorageStack{
		network:       net,
		minio:         minioCtr,
		minioEndpoint: minioEndpoint,
		cleanup: func() {
			_ = minioCtr.Terminate(ctx)
			_ = net.Remove(ctx)
		},
	}
	t.Cleanup(stack.cleanup)
	return stack
}

func ensureMinioBucket(ctx context.Context, networkName, bucket string) error {
	mcCtr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    "minio/mc",
			Networks: []string{networkName},
			Cmd: []string{
				"/bin/sh", "-c",
				fmt.Sprintf(
					"until mc alias set local http://minio:9000 %s %s; do sleep 1; done; mc mb local/%s --ignore-existing; mc anonymous set public local/%s",
					minioAccessKey, minioSecretKey, bucket, bucket,
				),
			},
		},
		Started: true,
	})
	if err != nil {
		return err
	}
	defer mcCtr.Terminate(ctx)

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		state, err := mcCtr.State(ctx)
		if err != nil {
			return err
		}
		if !state.Running {
			if state.ExitCode == 0 {
				return nil
			}
			return fmt.Errorf("mc init failed with exit code %d", state.ExitCode)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for mc bucket init")
}

func startIcebergRESTCatalog(ctx context.Context, t *testing.T, stack *objectStorageStack) (catalogURI string, container testcontainers.Container) {
	t.Helper()

	restCtr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/iceberg-rest-fixture:1.9.1",
			ExposedPorts: []string{"8181/tcp"},
			Networks:     []string{stack.network.Name},
			Env: map[string]string{
				"AWS_ACCESS_KEY_ID":     minioAccessKey,
				"AWS_SECRET_ACCESS_KEY": minioSecretKey,
				"AWS_REGION":            minioRegion,
				"CATALOG_WAREHOUSE":     "s3://" + minioBucket + "/",
				"CATALOG_IO__IMPL":      "org.apache.iceberg.aws.s3.S3FileIO",
				"CATALOG_S3_ENDPOINT":   "http://minio:9000",
			},
			WaitingFor: wait.ForHTTP("/v1/config").
				WithPort("8181/tcp").
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	requireDocker(t, err)
	t.Cleanup(func() { _ = restCtr.Terminate(ctx) })

	host, err := restCtr.Host(ctx)
	require.NoError(t, err)
	port, err := restCtr.MappedPort(ctx, "8181/tcp")
	require.NoError(t, err)

	return fmt.Sprintf("http://%s:%s", host, port.Port()), restCtr
}

func startNessieCatalog(ctx context.Context, t *testing.T, stack *objectStorageStack) (baseURL string, container testcontainers.Container) {
	t.Helper()

	nessieEnv := map[string]string{
		"nessie.version.store.type":                                   "IN_MEMORY",
		"nessie.catalog.default-warehouse":                            "warehouse",
		fmt.Sprintf("nessie.catalog.warehouses.warehouse.location"):   "s3://" + minioBucket + "/",
		"nessie.catalog.service.s3.default-options.region":            minioRegion,
		"nessie.catalog.service.s3.default-options.path-style-access": "true",
		"nessie.catalog.service.s3.default-options.endpoint":          "http://minio:9000/",
		"nessie.catalog.service.s3.default-options.external-endpoint": stack.minioEndpoint + "/",
		"nessie.catalog.service.s3.default-options.auth-type":         "STATIC",
		"nessie.catalog.service.s3.default-options.access-key":        "urn:nessie-secret:quarkus:nessie.catalog.secrets.access-key",
		"nessie.catalog.secrets.access-key.name":                      minioAccessKey,
		"nessie.catalog.secrets.access-key.secret":                    minioSecretKey,
		"nessie.server.authentication.enabled":                        "false",
	}

	nessieCtr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/projectnessie/nessie:0.99.0",
			ExposedPorts: []string{"19120/tcp"},
			Networks:     []string{stack.network.Name},
			Env:          nessieEnv,
			WaitingFor: wait.ForHTTP("/api/v2/config").
				WithPort("19120/tcp").
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	requireDocker(t, err)
	t.Cleanup(func() { _ = nessieCtr.Terminate(ctx) })

	host, err := nessieCtr.Host(ctx)
	require.NoError(t, err)
	port, err := nessieCtr.MappedPort(ctx, "19120/tcp")
	require.NoError(t, err)

	return fmt.Sprintf("http://%s:%s", host, port.Port()), nessieCtr
}
