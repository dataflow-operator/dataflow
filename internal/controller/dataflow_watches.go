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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// isOperatorDeployment returns true if obj is the operator Deployment (by name and namespace).
func (r *DataFlowReconciler) isOperatorDeployment(obj client.Object) bool {
	if r.operatorDeploymentName == "" || r.operatorDeploymentNamespace == "" {
		return false
	}
	return obj.GetNamespace() == r.operatorDeploymentNamespace && obj.GetName() == r.operatorDeploymentName
}

// enqueueAllDataFlowsForOperatorUpdate returns reconcile requests for all DataFlows (called when operator Deployment is updated).
func (r *DataFlowReconciler) enqueueAllDataFlowsForOperatorUpdate(ctx context.Context, o client.Object) []reconcile.Request {
	if !r.isOperatorDeployment(o) {
		return nil
	}
	list := &dataflowv1.DataFlowList{}
	if err := r.List(ctx, list); err != nil {
		log.FromContext(ctx).Error(err, "failed to list DataFlows for operator deployment update",
			"operatorDeploymentNamespace", o.GetNamespace(),
			"operatorDeploymentName", o.GetName(),
		)
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Name: list.Items[i].Name, Namespace: list.Items[i].Namespace,
		}})
	}
	return reqs
}

// shouldEnqueueOnOperatorDeploymentUpdate returns true only for meaningful operator Deployment updates.
func (r *DataFlowReconciler) shouldEnqueueOnOperatorDeploymentUpdate(oldObj, newObj client.Object) bool {
	oldDep, oldOK := oldObj.(*appsv1.Deployment)
	newDep, newOK := newObj.(*appsv1.Deployment)
	if !oldOK || !newOK {
		return false
	}
	if !r.isOperatorDeployment(newDep) {
		return false
	}

	// Generation changes on spec updates; template compare is an additional guard for clarity.
	return oldDep.Generation != newDep.Generation ||
		!equality.Semantic.DeepEqual(oldDep.Spec.Template, newDep.Spec.Template)
}

// enqueueAllDataFlowsForSecretUpdate returns reconcile requests for DataFlows affected by a Secret change:
// all DataFlows in the Secret's namespace (existing behavior for resolved secret refs in spec), plus any cluster-wide
// DataFlow whose Nessie sink S3 credential refs target this Secret (cross-namespace refs).
func (r *DataFlowReconciler) enqueueAllDataFlowsForSecretUpdate(ctx context.Context, o client.Object) []reconcile.Request {
	secret, ok := o.(*corev1.Secret)
	if !ok {
		return nil
	}
	if secret.Namespace == "" {
		return nil
	}
	seen := make(map[string]struct{})
	add := func(ns, name string, reqs *[]reconcile.Request) {
		key := ns + "/" + name
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		*reqs = append(*reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
	}

	var reqs []reconcile.Request

	listLocal := &dataflowv1.DataFlowList{}
	if err := r.List(ctx, listLocal, client.InNamespace(secret.Namespace)); err != nil {
		log.FromContext(ctx).Error(err, "failed to list DataFlows for secret update",
			"secretNamespace", secret.Namespace,
			"secretName", secret.Name,
		)
	} else {
		for i := range listLocal.Items {
			add(listLocal.Items[i].Namespace, listLocal.Items[i].Name, &reqs)
		}
	}

	listAll := &dataflowv1.DataFlowList{}
	if err := r.List(ctx, listAll); err != nil {
		log.FromContext(ctx).Error(err, "failed to list DataFlows for cross-namespace Nessie S3 secret watch",
			"secretNamespace", secret.Namespace,
			"secretName", secret.Name,
		)
		return reqs
	}
	for i := range listAll.Items {
		df := &listAll.Items[i]
		if catalogSinkObjectStorageRefsSecret(df, secret) {
			add(df.Namespace, df.Name, &reqs)
		}
	}
	return reqs
}

// shouldEnqueueOnSecretUpdate returns true only for meaningful Secret updates.
func (r *DataFlowReconciler) shouldEnqueueOnSecretUpdate(oldObj, newObj client.Object) bool {
	oldSecret, oldOK := oldObj.(*corev1.Secret)
	newSecret, newOK := newObj.(*corev1.Secret)
	if !oldOK || !newOK {
		return false
	}
	return !equality.Semantic.DeepEqual(oldSecret.Data, newSecret.Data) ||
		!equality.Semantic.DeepEqual(oldSecret.StringData, newSecret.StringData) ||
		!equality.Semantic.DeepEqual(oldSecret.Type, newSecret.Type)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DataFlowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&dataflowv1.DataFlow{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles(),
		})

	// When operator Deployment is updated, reconcile all DataFlows so processor pods get the new image.
	if r.operatorDeploymentName != "" && r.operatorDeploymentNamespace != "" {
		b = b.Watches(
			&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueAllDataFlowsForOperatorUpdate),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(_ event.CreateEvent) bool { return false },
				DeleteFunc:  func(_ event.DeleteEvent) bool { return false },
				GenericFunc: func(_ event.GenericEvent) bool { return false },
				UpdateFunc: func(e event.UpdateEvent) bool {
					return r.shouldEnqueueOnOperatorDeploymentUpdate(e.ObjectOld, e.ObjectNew)
				},
			}),
		)
	}

	// Watch Secret changes to refresh DataFlow specs with secret refs.
	if r.watchSecrets {
		b = b.Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueAllDataFlowsForSecretUpdate),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc: func(_ event.CreateEvent) bool { return true },
				DeleteFunc: func(_ event.DeleteEvent) bool { return true },
				GenericFunc: func(_ event.GenericEvent) bool {
					return false
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					return r.shouldEnqueueOnSecretUpdate(e.ObjectOld, e.ObjectNew)
				},
			}),
		)
	}

	return b.Complete(r)
}
