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

// nessieSinkUsesLocalObjectStorageSecretRefs is true when at least one S3 credential ref uses a Secret in the DataFlow namespace (processor SA needs get on those Secrets).
func nessieSinkUsesLocalObjectStorageSecretRefs(sink *dataflowv1.SinkSpec, dataflowNamespace string) bool {
	if !nessieSinkUsesObjectStorageSecretRefs(sink) {
		return false
	}
	cfg, err := sink.GetNessieConfig()
	if err != nil || cfg == nil {
		return false
	}
	return len(nessieSinkObjectStorageSecretNames(cfg, dataflowNamespace)) > 0
}

// nessieSinkUsesObjectStorageSecretRefs is true when both S3 credential secret refs are set (pod env injection).
func nessieSinkUsesObjectStorageSecretRefs(sink *dataflowv1.SinkSpec) bool {
	if sink == nil || !strings.EqualFold(sink.Type, "nessie") {
		return false
	}
	cfg, err := sink.GetNessieConfig()
	if err != nil || cfg == nil {
		return false
	}
	return cfg.AccessKeySecretRef != nil && cfg.SecretAccessKeySecretRef != nil
}

// effectiveSecretNamespace returns the namespace where the Secret lives for a ref (defaults to DataFlow namespace).
func effectiveSecretNamespace(ref *dataflowv1.SecretRef, dataflowNamespace string) string {
	if ref == nil || ref.Namespace == "" {
		return dataflowNamespace
	}
	return ref.Namespace
}

// validateNessieSinkObjectStorageRefs ensures S3 refs are paired and well-formed (name/key).
// Cross-namespace refs are allowed: values are resolved by the operator into Deployment env literals.
func validateNessieSinkObjectStorageRefs(sink *dataflowv1.SinkSpec) error {
	if sink == nil || !strings.EqualFold(sink.Type, "nessie") {
		return nil
	}
	cfg, err := sink.GetNessieConfig()
	if err != nil {
		return fmt.Errorf("nessie sink: invalid config: %w", err)
	}
	if cfg == nil {
		return nil
	}
	a := cfg.AccessKeySecretRef
	s := cfg.SecretAccessKeySecretRef
	if a == nil && s == nil {
		return nil
	}
	if (a == nil) != (s == nil) {
		return fmt.Errorf("nessie sink: accessKeySecretRef and secretAccessKeySecretRef must both be set or both omitted")
	}
	if err := validateNessieS3SecretRefShape(a, "accessKeySecretRef"); err != nil {
		return err
	}
	if err := validateNessieS3SecretRefShape(s, "secretAccessKeySecretRef"); err != nil {
		return err
	}
	return nil
}

func validateNessieS3SecretRefShape(ref *dataflowv1.SecretRef, field string) error {
	if ref == nil {
		return fmt.Errorf("nessie sink: %s is nil", field)
	}
	if ref.Name == "" || ref.Key == "" {
		return fmt.Errorf("nessie sink: %s must specify name and key", field)
	}
	return nil
}

// nessieSinkObjectStorageEnvWithResolve builds processor env for iceberg-go / AWS SDK.
// Refs in the DataFlow namespace use secretKeyRef; refs in other namespaces are resolved to literals via the operator client.
func nessieSinkObjectStorageEnvWithResolve(ctx context.Context, resolver *SecretResolver, dataflowNamespace string, cfg *dataflowv1.NessieSinkSpec) ([]corev1.EnvVar, error) {
	if cfg == nil || cfg.AccessKeySecretRef == nil || cfg.SecretAccessKeySecretRef == nil {
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
			return corev1.EnvVar{}, fmt.Errorf("nessie sink object storage %s: %w", envName, err)
		}
		return corev1.EnvVar{Name: envName, Value: val}, nil
	}
	ak, err := envOne(envAWSAccessKeyID, cfg.AccessKeySecretRef)
	if err != nil {
		return nil, err
	}
	sk, err := envOne(envAWSSecretAccessKey, cfg.SecretAccessKeySecretRef)
	if err != nil {
		return nil, err
	}
	out := []corev1.EnvVar{ak, sk}
	if cfg.S3Endpoint != "" {
		out = append(out, corev1.EnvVar{Name: envAWSS3Endpoint, Value: cfg.S3Endpoint})
	}
	if cfg.S3Region != "" {
		out = append(out, corev1.EnvVar{Name: envAWSRegion, Value: cfg.S3Region})
	}
	return out, nil
}

// nessieSinkObjectStorageSecretNames returns distinct secret names for Role resourceNames (same-namespace refs only).
func nessieSinkObjectStorageSecretNames(cfg *dataflowv1.NessieSinkSpec, dataflowNamespace string) []string {
	if cfg == nil || cfg.AccessKeySecretRef == nil || cfg.SecretAccessKeySecretRef == nil {
		return nil
	}
	names := map[string]struct{}{}
	for _, ref := range []*dataflowv1.SecretRef{cfg.AccessKeySecretRef, cfg.SecretAccessKeySecretRef} {
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

// nessieSinkObjectStorageRefsSecret reports whether the DataFlow Nessie sink S3 credential refs point at this Secret.
func nessieSinkObjectStorageRefsSecret(df *dataflowv1.DataFlow, secret *corev1.Secret) bool {
	if df == nil || secret == nil {
		return false
	}
	if !strings.EqualFold(df.Spec.Sink.Type, "nessie") {
		return false
	}
	cfg, err := df.Spec.Sink.GetNessieConfig()
	if err != nil || cfg == nil {
		return false
	}
	match := func(ref *dataflowv1.SecretRef) bool {
		if ref == nil || ref.Name != secret.Name {
			return false
		}
		return effectiveSecretNamespace(ref, df.Namespace) == secret.Namespace
	}
	return match(cfg.AccessKeySecretRef) || match(cfg.SecretAccessKeySecretRef)
}
