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

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

func checkpointPersistenceEnabled(spec *dataflowv1.DataFlowSpec) bool {
	return spec == nil || spec.CheckpointPersistence == nil || *spec.CheckpointPersistence
}

func processorNeedsDedicatedServiceAccount(resolvedSpec *dataflowv1.DataFlowSpec, namespace string) bool {
	if resolvedSpec == nil {
		return false
	}
	if checkpointPersistenceEnabled(resolvedSpec) {
		return true
	}
	return nessieSinkUsesLocalObjectStorageSecretRefs(&resolvedSpec.Sink, namespace)
}

func processorServiceAccountName(workflowName string, resolvedSpec *dataflowv1.DataFlowSpec, namespace string) string {
	if processorNeedsDedicatedServiceAccount(resolvedSpec, namespace) {
		return k8snames.ProcessorServiceAccount(workflowName)
	}
	return ""
}

func createOrUpdateCheckpointConfigMap(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	namespace, workflowName string,
	owner metav1.Object,
) (created bool, err error) {
	log := log.FromContext(ctx)
	configMapName := k8snames.ProcessorCheckpointConfigMap(workflowName)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: namespace,
		},
		Data: map[string]string{},
	}
	if err := ctrl.SetControllerReference(owner, configMap, scheme); err != nil {
		return false, fmt.Errorf("failed to set controller reference: %w", err)
	}
	existing := &corev1.ConfigMap{}
	getErr := c.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: namespace}, existing)
	if getErr != nil && apierrors.IsNotFound(getErr) {
		if err := c.Create(ctx, configMap); err != nil {
			return false, fmt.Errorf("failed to create checkpoint ConfigMap: %w", err)
		}
		log.Info("Created checkpoint ConfigMap", "name", configMapName)
		return true, nil
	}
	if getErr != nil {
		return false, fmt.Errorf("failed to get checkpoint ConfigMap: %w", getErr)
	}
	return false, nil
}

func createOrUpdateProcessorRBAC(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	namespace, workflowName string,
	owner metav1.Object,
	resolvedSpec *dataflowv1.DataFlowSpec,
) error {
	log := log.FromContext(ctx)
	saName := k8snames.ProcessorServiceAccount(workflowName)
	roleName := saName
	configMapName := k8snames.ProcessorCheckpointConfigMap(workflowName)

	var rules []rbacv1.PolicyRule
	if resolvedSpec != nil && checkpointPersistenceEnabled(resolvedSpec) {
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups:     []string{""},
			Resources:     []string{"configmaps"},
			ResourceNames: []string{configMapName},
			Verbs:         []string{"get", "patch", "update"},
		})
	}
	if resolvedSpec != nil {
		if cfg, err := resolvedSpec.Sink.GetNessieConfig(); err == nil && cfg != nil {
			if secretNames := nessieSinkObjectStorageSecretNames(cfg, namespace); len(secretNames) > 0 {
				rules = append(rules, rbacv1.PolicyRule{
					APIGroups:     []string{""},
					Resources:     []string{"secrets"},
					ResourceNames: secretNames,
					Verbs:         []string{"get"},
				})
			}
		}
	}

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: namespace,
		},
	}
	if err := ctrl.SetControllerReference(owner, sa, scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on ServiceAccount: %w", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: saName, Namespace: namespace}, &corev1.ServiceAccount{}); err != nil {
		if apierrors.IsNotFound(err) {
			if err := c.Create(ctx, sa); err != nil {
				return fmt.Errorf("failed to create ServiceAccount: %w", err)
			}
			log.Info("Created processor ServiceAccount", "name", saName)
		} else {
			return err
		}
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: namespace,
		},
		Rules: rules,
	}
	if err := ctrl.SetControllerReference(owner, role, scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on Role: %w", err)
	}
	existingRole := &rbacv1.Role{}
	if err := c.Get(ctx, types.NamespacedName{Name: roleName, Namespace: namespace}, existingRole); err != nil {
		if apierrors.IsNotFound(err) {
			if err := c.Create(ctx, role); err != nil {
				return fmt.Errorf("failed to create Role: %w", err)
			}
			log.Info("Created processor Role", "name", roleName)
		} else {
			return err
		}
	} else {
		existingRole.Rules = role.Rules
		if err := c.Update(ctx, existingRole); err != nil {
			return fmt.Errorf("failed to update Role: %w", err)
		}
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      saName,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     roleName,
		},
	}
	if err := ctrl.SetControllerReference(owner, binding, scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on RoleBinding: %w", err)
	}
	existingBinding := &rbacv1.RoleBinding{}
	if err := c.Get(ctx, types.NamespacedName{Name: roleName, Namespace: namespace}, existingBinding); err != nil {
		if apierrors.IsNotFound(err) {
			if err := c.Create(ctx, binding); err != nil {
				return fmt.Errorf("failed to create RoleBinding: %w", err)
			}
			log.Info("Created processor RoleBinding", "name", roleName)
		} else {
			return err
		}
	} else {
		existingBinding.Subjects = binding.Subjects
		existingBinding.RoleRef = binding.RoleRef
		if err := c.Update(ctx, existingBinding); err != nil {
			return fmt.Errorf("failed to update RoleBinding: %w", err)
		}
	}

	return nil
}

func deleteProcessorCheckpointAndRBAC(ctx context.Context, c client.Client, namespace, workflowName string) error {
	log := log.FromContext(ctx)
	checkpointConfigMapName := k8snames.ProcessorCheckpointConfigMap(workflowName)
	saName := k8snames.ProcessorServiceAccount(workflowName)

	checkpointCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: checkpointConfigMapName, Namespace: namespace},
	}
	if err := c.Delete(ctx, checkpointCM); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	log.V(1).Info("Deleted checkpoint ConfigMap", "name", checkpointConfigMapName)

	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace},
	}
	if err := c.Delete(ctx, roleBinding); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace},
	}
	if err := c.Delete(ctx, role); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace},
	}
	if err := c.Delete(ctx, sa); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}
