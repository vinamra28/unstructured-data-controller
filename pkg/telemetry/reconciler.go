/*
Copyright 2026.

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

package telemetry

import (
	"context"
	"errors"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// errReconcilePanic is recorded when Reconcile unwinds from a panic. The panic
// value itself is deliberately not used: it can embed arbitrary downstream
// data, and controller-runtime already logs it at its own recover point.
var errReconcilePanic = errors.New("reconcile panicked")

// observedReconciler records reconcile telemetry around an inner Reconciler.
type observedReconciler struct {
	provider       *Provider
	controllerName string
	inner          reconcile.Reconciler
}

// ObserveReconciler wraps inner so every Reconcile call records the reconcile
// count, duration, and error metrics under controllerName. Apply it where the
// reconciler is handed to the builder:
//
//	Complete(r.Metrics.ObserveReconciler(MyControllerName, r))
//
// Decorating the Reconciler keeps telemetry out of the reconcile bodies, and
// unlike a defer inside Reconcile it also counts panics. controller-runtime's
// recover() sits above this decorator, so a panicking reconcile unwinds past a
// defer inside Reconcile while the named error return is still nil, and would
// otherwise be counted as a success.
//
// Safe to call on a nil Provider; inner is then returned undecorated.
func (p *Provider) ObserveReconciler(controllerName string, inner reconcile.Reconciler) reconcile.Reconciler {
	if p == nil || p.instruments == nil {
		return inner
	}
	return &observedReconciler{
		provider:       p,
		controllerName: controllerName,
		inner:          inner,
	}
}

// Reconcile implements reconcile.Reconciler.
func (o *observedReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	observer := o.provider.ReconcileObserver(o.controllerName)

	// completed separates a normal return from a panic unwind. Recovering here
	// would discard the stack trace controller-runtime logs at its own recover
	// point, so the panic is left to propagate and is only observed on the way
	// past.
	completed := false
	var retErr error
	defer func() {
		if !completed {
			panicErr := errReconcilePanic
			observer.End(ctx, &panicErr)
			return
		}
		observer.End(ctx, &retErr)
	}()

	result, err := o.inner.Reconcile(ctx, req)
	retErr = err
	completed = true
	return result, err
}
