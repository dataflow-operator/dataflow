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

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

const (
	envAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	envAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	envAWSS3Endpoint      = "AWS_S3_ENDPOINT"
	envAWSRegion          = "AWS_REGION"
)

type catalogSinkObjectStorage struct {
	accessKeySecretRef       *dataflowv1.SecretRef
	secretAccessKeySecretRef *dataflowv1.SecretRef
	s3Endpoint               string
	s3Region                 string
}

func catalogSinkObjectStorageFromSink(sink *dataflowv1.SinkSpec) (catalogSinkObjectStorage, string, bool) {
	if sink == nil {
		return catalogSinkObjectStorage{}, "", false
	}
	switch strings.ToLower(sink.Type) {
	case "nessie":
		cfg, err := sink.GetNessieConfig()
		if err != nil || cfg == nil {
			return catalogSinkObjectStorage{}, "", false
		}
		return catalogSinkObjectStorage{
			accessKeySecretRef:       cfg.AccessKeySecretRef,
			secretAccessKeySecretRef: cfg.SecretAccessKeySecretRef,
			s3Endpoint:               cfg.S3Endpoint,
			s3Region:                 cfg.S3Region,
		}, "nessie", true
	case "iceberg":
		cfg, err := sink.GetIcebergConfig()
		if err != nil || cfg == nil {
			return catalogSinkObjectStorage{}, "", false
		}
		return catalogSinkObjectStorage{
			accessKeySecretRef:       cfg.AccessKeySecretRef,
			secretAccessKeySecretRef: cfg.SecretAccessKeySecretRef,
			s3Endpoint:               cfg.S3Endpoint,
			s3Region:                 cfg.S3Region,
		}, "iceberg", true
	default:
		return catalogSinkObjectStorage{}, "", false
	}
}

func catalogSinkUsesObjectStorageSecretRefs(sink *dataflowv1.SinkSpec) bool {
	cfg, _, ok := catalogSinkObjectStorageFromSink(sink)
	if !ok {
		return false
	}
	return cfg.accessKeySecretRef != nil && cfg.secretAccessKeySecretRef != nil
}

func catalogSinkUsesLocalObjectStorageSecretRefs(sink *dataflowv1.SinkSpec, dataflowNamespace string) bool {
	if !catalogSinkUsesObjectStorageSecretRefs(sink) {
		return false
	}
	cfg, _, ok := catalogSinkObjectStorageFromSink(sink)
	if !ok {
		return false
	}
	return len(catalogSinkObjectStorageSecretNames(cfg, dataflowNamespace)) > 0
}

func effectiveSecretNamespace(ref *dataflowv1.SecretRef, dataflowNamespace string) string {
	if ref == nil || ref.Namespace == "" {
		return dataflowNamespace
	}
	return ref.Namespace
}

func validateCatalogSinkObjectStorageRefs(sink *dataflowv1.SinkSpec) error {
	cfg, label, ok := catalogSinkObjectStorageFromSink(sink)
	if !ok {
		return nil
	}
	a := cfg.accessKeySecretRef
	s := cfg.secretAccessKeySecretRef
	if a == nil && s == nil {
		return nil
	}
	if (a == nil) != (s == nil) {
		return fmt.Errorf("%s sink: accessKeySecretRef and secretAccessKeySecretRef must both be set or both omitted", label)
	}
	if err := validateCatalogS3SecretRefShape(a, label, "accessKeySecretRef"); err != nil {
		return err
	}
	if err := validateCatalogS3SecretRefShape(s, label, "secretAccessKeySecretRef"); err != nil {
		return err
	}
	return nil
}

func validateCatalogS3SecretRefShape(ref *dataflowv1.SecretRef, sinkLabel, field string) error {
	if ref == nil {
		return fmt.Errorf("%s sink: %s is nil", sinkLabel, field)
	}
	if ref.Name == "" || ref.Key == "" {
		return fmt.Errorf("%s sink: %s must specify name and key", sinkLabel, field)
	}
	return nil
}

func catalogSinkObjectStorageEnvWithResolve(ctx context.Context, resolver *SecretResolver, dataflowNamespace string, sink *dataflowv1.SinkSpec) ([]corev1.EnvVar, error) {
	cfg, label, ok := catalogSinkObjectStorageFromSink(sink)
	if !ok {
		return nil, nil
	}
	return catalogSinkObjectStorageEnvFromConfig(ctx, resolver, dataflowNamespace, cfg, label)
}

func catalogSinkObjectStorageEnvFromConfig(ctx context.Context, resolver *SecretResolver, dataflowNamespace string, cfg catalogSinkObjectStorage, label string) ([]corev1.EnvVar, error) {
	if cfg.accessKeySecretRef == nil || cfg.secretAccessKeySecretRef == nil {
		return nil, nil
	}
	envOne := func(envName string, ref *dataflowv1.SecretRef) (corev1.EnvVar, error) {
		if effectiveSecretNamespace(ref, dataflowNamespace) == dataflowNamespace {
			return corev1.EnvVar{
				Name: envName,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
						Key:                  ref.Key,
					},
				},
			}, nil
		}
		val, err := resolver.ResolveSecretValue(ctx, dataflowNamespace, ref)
		if err != nil {
			return corev1.EnvVar{}, fmt.Errorf("%s sink object storage %s: %w", label, envName, err)
		}
		return corev1.EnvVar{Name: envName, Value: val}, nil
	}
	ak, err := envOne(envAWSAccessKeyID, cfg.accessKeySecretRef)
	if err != nil {
		return nil, err
	}
	sk, err := envOne(envAWSSecretAccessKey, cfg.secretAccessKeySecretRef)
	if err != nil {
		return nil, err
	}
	out := []corev1.EnvVar{ak, sk}
	if cfg.s3Endpoint != "" {
		out = append(out, corev1.EnvVar{Name: envAWSS3Endpoint, Value: cfg.s3Endpoint})
	}
	if cfg.s3Region != "" {
		out = append(out, corev1.EnvVar{Name: envAWSRegion, Value: cfg.s3Region})
	}
	return out, nil
}

func catalogSinkObjectStorageSecretNames(cfg catalogSinkObjectStorage, dataflowNamespace string) []string {
	if cfg.accessKeySecretRef == nil || cfg.secretAccessKeySecretRef == nil {
		return nil
	}
	names := map[string]struct{}{}
	for _, ref := range []*dataflowv1.SecretRef{cfg.accessKeySecretRef, cfg.secretAccessKeySecretRef} {
		if ref == nil || ref.Name == "" {
			continue
		}
		if effectiveSecretNamespace(ref, dataflowNamespace) != dataflowNamespace {
			continue
		}
		names[ref.Name] = struct{}{}
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func catalogSinkObjectStorageRefsSecret(df *dataflowv1.DataFlow, secret *corev1.Secret) bool {
	if df == nil || secret == nil {
		return false
	}
	cfg, _, ok := catalogSinkObjectStorageFromSink(&df.Spec.Sink)
	if !ok {
		return false
	}
	match := func(ref *dataflowv1.SecretRef) bool {
		if ref == nil || ref.Name != secret.Name {
			return false
		}
		return effectiveSecretNamespace(ref, df.Namespace) == secret.Namespace
	}
	return match(cfg.accessKeySecretRef) || match(cfg.secretAccessKeySecretRef)
}

func nessieSinkUsesLocalObjectStorageSecretRefs(sink *dataflowv1.SinkSpec, dataflowNamespace string) bool {
	return catalogSinkUsesLocalObjectStorageSecretRefs(sink, dataflowNamespace)
}

func nessieSinkUsesObjectStorageSecretRefs(sink *dataflowv1.SinkSpec) bool {
	return catalogSinkUsesObjectStorageSecretRefs(sink)
}

func validateNessieSinkObjectStorageRefs(sink *dataflowv1.SinkSpec) error {
	return validateCatalogSinkObjectStorageRefs(sink)
}

func validateNessieS3SecretRefShape(ref *dataflowv1.SecretRef, field string) error {
	return validateCatalogS3SecretRefShape(ref, "nessie", field)
}

func nessieSinkObjectStorageEnvWithResolve(ctx context.Context, resolver *SecretResolver, dataflowNamespace string, cfg *dataflowv1.NessieSinkSpec) ([]corev1.EnvVar, error) {
	if cfg == nil {
		return nil, nil
	}
	return catalogSinkObjectStorageEnvFromConfig(ctx, resolver, dataflowNamespace, catalogSinkObjectStorage{
		accessKeySecretRef:       cfg.AccessKeySecretRef,
		secretAccessKeySecretRef: cfg.SecretAccessKeySecretRef,
		s3Endpoint:               cfg.S3Endpoint,
		s3Region:                 cfg.S3Region,
	}, "nessie")
}

func nessieSinkObjectStorageSecretNames(cfg *dataflowv1.NessieSinkSpec, dataflowNamespace string) []string {
	if cfg == nil {
		return nil
	}
	return catalogSinkObjectStorageSecretNames(catalogSinkObjectStorage{
		accessKeySecretRef:       cfg.AccessKeySecretRef,
		secretAccessKeySecretRef: cfg.SecretAccessKeySecretRef,
	}, dataflowNamespace)
}

func nessieSinkObjectStorageRefsSecret(df *dataflowv1.DataFlow, secret *corev1.Secret) bool {
	return catalogSinkObjectStorageRefsSecret(df, secret)
}
