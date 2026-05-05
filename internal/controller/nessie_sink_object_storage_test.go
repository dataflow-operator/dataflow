/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateNessieSinkObjectStorageRefs_okBothRefs(t *testing.T) {
	sink := dataflowv1.SinkSpec{
		Type: "nessie",
		Config: mustConfig(dataflowv1.NessieSinkSpec{
			BaseURL:   "https://nessie.example",
			Namespace: "ns",
			Table:     "t",
			AccessKeySecretRef: &dataflowv1.SecretRef{
				Name: "s3-creds", Key: "AWS_ACCESS_KEY_ID",
			},
			SecretAccessKeySecretRef: &dataflowv1.SecretRef{
				Name: "s3-creds", Key: "AWS_SECRET_ACCESS_KEY",
			},
		}),
	}
	require.NoError(t, validateNessieSinkObjectStorageRefs(&sink))
}

func TestValidateNessieSinkObjectStorageRefs_partialRefs(t *testing.T) {
	sink := dataflowv1.SinkSpec{
		Type: "nessie",
		Config: mustConfig(dataflowv1.NessieSinkSpec{
			BaseURL:   "https://nessie.example",
			Namespace: "ns",
			Table:     "t",
			AccessKeySecretRef: &dataflowv1.SecretRef{
				Name: "s3-creds", Key: "k",
			},
		}),
	}
	err := validateNessieSinkObjectStorageRefs(&sink)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both be set")
}

func TestValidateNessieSinkObjectStorageRefs_crossNamespaceAllowed(t *testing.T) {
	sink := dataflowv1.SinkSpec{
		Type: "nessie",
		Config: mustConfig(dataflowv1.NessieSinkSpec{
			BaseURL:   "https://nessie.example",
			Namespace: "ns",
			Table:     "t",
			AccessKeySecretRef: &dataflowv1.SecretRef{
				Name: "s3-creds", Namespace: "other", Key: "AWS_ACCESS_KEY_ID",
			},
			SecretAccessKeySecretRef: &dataflowv1.SecretRef{
				Name: "s3-creds", Namespace: "other", Key: "AWS_SECRET_ACCESS_KEY",
			},
		}),
	}
	require.NoError(t, validateNessieSinkObjectStorageRefs(&sink))
}

func TestNessieSinkObjectStorageEnvWithResolve_localUsesSecretKeyRef(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	resolver := NewSecretResolver(fakeClient)
	cfg := &dataflowv1.NessieSinkSpec{
		S3Endpoint: "https://storage.example.net",
		S3Region:   "ru-central1",
		AccessKeySecretRef: &dataflowv1.SecretRef{
			Name: "iceberg-s3", Key: "access",
		},
		SecretAccessKeySecretRef: &dataflowv1.SecretRef{
			Name: "iceberg-s3", Key: "secret",
		},
	}
	env, err := nessieSinkObjectStorageEnvWithResolve(context.Background(), resolver, "default", cfg)
	require.NoError(t, err)
	require.Len(t, env, 4)
	assert.Equal(t, envAWSAccessKeyID, env[0].Name)
	require.NotNil(t, env[0].ValueFrom.SecretKeyRef)
	assert.Equal(t, "iceberg-s3", env[0].ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, "access", env[0].ValueFrom.SecretKeyRef.Key)
	assert.Equal(t, envAWSSecretAccessKey, env[1].Name)
	assert.Equal(t, envAWSS3Endpoint, env[2].Name)
	assert.Equal(t, "https://storage.example.net", env[2].Value)
	assert.Equal(t, envAWSRegion, env[3].Name)
}

func TestNessieSinkObjectStorageEnvWithResolve_remoteUsesLiteralValue(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "remote-s3", Namespace: "other"},
		Data: map[string][]byte{
			"k1": []byte("AKIAEXAMPLE"),
			"k2": []byte("secretkey"),
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sec).Build()
	resolver := NewSecretResolver(fakeClient)
	cfg := &dataflowv1.NessieSinkSpec{
		AccessKeySecretRef: &dataflowv1.SecretRef{
			Name: "remote-s3", Namespace: "other", Key: "k1",
		},
		SecretAccessKeySecretRef: &dataflowv1.SecretRef{
			Name: "remote-s3", Namespace: "other", Key: "k2",
		},
	}
	env, err := nessieSinkObjectStorageEnvWithResolve(context.Background(), resolver, "default", cfg)
	require.NoError(t, err)
	require.Len(t, env, 2)
	assert.Equal(t, envAWSAccessKeyID, env[0].Name)
	assert.Nil(t, env[0].ValueFrom)
	assert.Equal(t, "AKIAEXAMPLE", env[0].Value)
	assert.Equal(t, envAWSSecretAccessKey, env[1].Name)
	assert.Equal(t, "secretkey", env[1].Value)
}

func TestNessieSinkObjectStorageSecretNames(t *testing.T) {
	cfg := &dataflowv1.NessieSinkSpec{
		AccessKeySecretRef:       &dataflowv1.SecretRef{Name: "a", Key: "k1"},
		SecretAccessKeySecretRef: &dataflowv1.SecretRef{Name: "b", Key: "k2"},
	}
	names := nessieSinkObjectStorageSecretNames(cfg, "default")
	assert.Equal(t, []string{"a", "b"}, names)
}

func TestNessieSinkObjectStorageSecretNames_skipsRemoteRefs(t *testing.T) {
	cfg := &dataflowv1.NessieSinkSpec{
		AccessKeySecretRef:       &dataflowv1.SecretRef{Name: "a", Namespace: "other", Key: "k1"},
		SecretAccessKeySecretRef: &dataflowv1.SecretRef{Name: "b", Namespace: "other", Key: "k2"},
	}
	assert.Empty(t, nessieSinkObjectStorageSecretNames(cfg, "default"))
}

func TestNessieSinkObjectStorageRefsSecret(t *testing.T) {
	df := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "dataflow"},
		Spec: dataflowv1.DataFlowSpec{
			Sink: dataflowv1.SinkSpec{
				Type: "nessie",
				Config: mustConfig(dataflowv1.NessieSinkSpec{
					BaseURL:                  "https://n",
					Namespace:                "ns",
					Table:                    "t",
					AccessKeySecretRef:       &dataflowv1.SecretRef{Name: "s", Namespace: "trino", Key: "k"},
					SecretAccessKeySecretRef: &dataflowv1.SecretRef{Name: "s", Namespace: "trino", Key: "k2"},
				}),
			},
		},
	}
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "trino"}}
	assert.True(t, nessieSinkObjectStorageRefsSecret(df, sec))
	secWrong := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "trino"}}
	assert.False(t, nessieSinkObjectStorageRefsSecret(df, secWrong))
}

func envVarByName(env []corev1.EnvVar, name string) (corev1.EnvVar, bool) {
	for _, e := range env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

func TestDataFlowReconciler_enqueueAllDataFlowsForSecretUpdate_crossNamespaceNessie(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	df := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-a", Namespace: "dataflow"},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type: "nessie",
				Config: mustConfig(dataflowv1.NessieSinkSpec{
					BaseURL: "https://n", Namespace: "ns", Table: "t",
					AccessKeySecretRef:       &dataflowv1.SecretRef{Name: "warehouse", Namespace: "trino", Key: "a"},
					SecretAccessKeySecretRef: &dataflowv1.SecretRef{Name: "warehouse", Namespace: "trino", Key: "b"},
				}),
			},
		},
	}
	dfUnrelated := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-b", Namespace: "dataflow"},
		Spec: dataflowv1.DataFlowSpec{
			Source: df.Spec.Source,
			Sink:   dataflowv1.SinkSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "out"})},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(df, dfUnrelated).Build()
	r := NewDataFlowReconciler(fakeClient, scheme, nil)
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "warehouse", Namespace: "trino"}}
	reqs := r.enqueueAllDataFlowsForSecretUpdate(context.Background(), sec)
	names := map[string]bool{}
	for _, req := range reqs {
		names[req.Namespace+"/"+req.Name] = true
	}
	assert.True(t, names["dataflow/flow-a"], "expected DataFlow with Nessie S3 ref to trino/warehouse")
}
