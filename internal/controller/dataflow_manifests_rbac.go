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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

// createOrUpdateProcessorRBAC creates ServiceAccount, Role, and RoleBinding for the processor pod:
// checkpoint ConfigMap access when checkpoint persistence is enabled, and Secret get when Nessie sink uses in-namespace S3 refs.
func (r *DataFlowReconciler) createOrUpdateProcessorRBAC(ctx context.Context, req ctrl.Request, dataflow *dataflowv1.DataFlow, resolvedSpec *dataflowv1.DataFlowSpec) error {
	log := log.FromContext(ctx)
	saName := k8snames.ProcessorServiceAccount(dataflow.Name)
	roleName := saName
	configMapName := k8snames.ProcessorCheckpointConfigMap(dataflow.Name)

	var rules []rbacv1.PolicyRule
	if resolvedSpec != nil && (resolvedSpec.CheckpointPersistence == nil || *resolvedSpec.CheckpointPersistence) {
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups:     []string{""},
			Resources:     []string{"configmaps"},
			ResourceNames: []string{configMapName},
			Verbs:         []string{"get", "patch", "update"},
		})
	}
	if resolvedSpec != nil {
		if cfg, err := resolvedSpec.Sink.GetNessieConfig(); err == nil && cfg != nil {
			if secretNames := nessieSinkObjectStorageSecretNames(cfg, req.Namespace); len(secretNames) > 0 {
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
			Namespace: req.Namespace,
		},
	}
	if err := ctrl.SetControllerReference(dataflow, sa, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on ServiceAccount: %w", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Name: saName, Namespace: req.Namespace}, &corev1.ServiceAccount{}); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, sa); err != nil {
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
			Namespace: req.Namespace,
		},
		Rules: rules,
	}
	if err := ctrl.SetControllerReference(dataflow, role, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on Role: %w", err)
	}
	existingRole := &rbacv1.Role{}
	if err := r.Get(ctx, types.NamespacedName{Name: roleName, Namespace: req.Namespace}, existingRole); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, role); err != nil {
				return fmt.Errorf("failed to create Role: %w", err)
			}
			log.Info("Created processor Role", "name", roleName)
		} else {
			return err
		}
	} else {
		existingRole.Rules = role.Rules
		if err := r.Update(ctx, existingRole); err != nil {
			return fmt.Errorf("failed to update Role: %w", err)
		}
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: req.Namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      saName,
				Namespace: req.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     roleName,
		},
	}
	if err := ctrl.SetControllerReference(dataflow, binding, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on RoleBinding: %w", err)
	}
	existingBinding := &rbacv1.RoleBinding{}
	if err := r.Get(ctx, types.NamespacedName{Name: roleName, Namespace: req.Namespace}, existingBinding); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, binding); err != nil {
				return fmt.Errorf("failed to create RoleBinding: %w", err)
			}
			log.Info("Created processor RoleBinding", "name", roleName)
		} else {
			return err
		}
	} else {
		existingBinding.Subjects = binding.Subjects
		existingBinding.RoleRef = binding.RoleRef
		if err := r.Update(ctx, existingBinding); err != nil {
			return fmt.Errorf("failed to update RoleBinding: %w", err)
		}
	}

	return nil
}
