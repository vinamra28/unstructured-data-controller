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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// requireControllerAttr asserts a data point is attributed to controllerName.
func requireControllerAttr(t *testing.T, attrs attribute.Set, controllerName string) {
	t.Helper()
	value, ok := attrs.Value(attribute.Key("controller"))
	require.True(t, ok, "data point should carry a controller attribute")
	assert.Equal(t, controllerName, value.AsString())
}

func TestObserveReconcilerRecordsSuccess(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	expectedResult := reconcile.Result{RequeueAfter: 30 * time.Second}
	inner := reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return expectedResult, nil
	})

	observed := p.ObserveReconciler("TestController", inner)
	result, err := observed.Reconcile(context.Background(), reconcile.Request{})

	// The decorator must pass the inner result and error through untouched.
	require.NoError(t, err)
	assert.Equal(t, expectedResult, result)

	rm := collectMetrics(t, reader)

	countSum := requireSumMetric(t, rm, "udc.reconcile.count")
	require.Len(t, countSum.DataPoints, 1)
	assert.Equal(t, int64(1), countSum.DataPoints[0].Value)
	requireControllerAttr(t, countSum.DataPoints[0].Attributes, "TestController")

	durationHist := requireHistogramMetric(t, rm, "udc.reconcile.duration")
	require.Len(t, durationHist.DataPoints, 1)
	assert.Equal(t, uint64(1), durationHist.DataPoints[0].Count)

	assert.Nil(t, findMetric(rm, "udc.reconcile.errors"),
		"error counter should not be recorded on success")
}

func TestObserveReconcilerRecordsError(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	expectedErr := errors.New("reconcile failed")
	inner := reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return reconcile.Result{}, expectedErr
	})

	observed := p.ObserveReconciler("FailingController", inner)
	_, err := observed.Reconcile(context.Background(), reconcile.Request{})

	// The original error must reach controller-runtime unwrapped so its
	// backoff and logging behave exactly as they would without the decorator.
	require.ErrorIs(t, err, expectedErr)

	rm := collectMetrics(t, reader)

	errSum := requireSumMetric(t, rm, "udc.reconcile.errors")
	require.Len(t, errSum.DataPoints, 1)
	assert.Equal(t, int64(1), errSum.DataPoints[0].Value)
	requireControllerAttr(t, errSum.DataPoints[0].Attributes, "FailingController")

	countSum := requireSumMetric(t, rm, "udc.reconcile.count")
	require.Len(t, countSum.DataPoints, 1)
	assert.Equal(t, int64(1), countSum.DataPoints[0].Value)
}

// TestObserveReconcilerRecordsPanic covers the case a defer inside Reconcile
// cannot: controller-runtime's recover() sits above the reconciler, so a panic
// unwinds with a nil error return and would otherwise count as a success.
func TestObserveReconcilerRecordsPanic(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	inner := reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		panic("boom")
	})

	observed := p.ObserveReconciler("PanickingController", inner)

	// The panic must still propagate: recovering here would discard the stack
	// trace controller-runtime logs at its own recover point.
	require.PanicsWithValue(t, "boom", func() {
		_, _ = observed.Reconcile(context.Background(), reconcile.Request{})
	})

	rm := collectMetrics(t, reader)

	errSum := requireSumMetric(t, rm, "udc.reconcile.errors")
	require.Len(t, errSum.DataPoints, 1)
	assert.Equal(t, int64(1), errSum.DataPoints[0].Value)
	requireControllerAttr(t, errSum.DataPoints[0].Attributes, "PanickingController")

	countSum := requireSumMetric(t, rm, "udc.reconcile.count")
	require.Len(t, countSum.DataPoints, 1)
	assert.Equal(t, int64(1), countSum.DataPoints[0].Value)
}

func TestObserveReconcilerNilProviderReturnsInner(t *testing.T) {
	expectedResult := reconcile.Result{RequeueAfter: time.Minute}
	inner := reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return expectedResult, nil
	})

	// A nil Provider is how reconcilers run with telemetry disabled, so the
	// decorator must hand back the reconciler untouched rather than wrap it.
	var p *Provider
	observed := p.ObserveReconciler("AnyController", inner)

	require.NotPanics(t, func() {
		result, err := observed.Reconcile(context.Background(), reconcile.Request{})
		require.NoError(t, err)
		assert.Equal(t, expectedResult, result)
	})
}

// TestObserveReconcilerUninitialisedProviderReturnsInner guards the case an
// external consumer or test can construct: Provider has unexported fields, so
// &Provider{} is the only literal available and its instruments are nil.
func TestObserveReconcilerUninitialisedProviderReturnsInner(t *testing.T) {
	inner := reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return reconcile.Result{}, nil
	})

	observed := (&Provider{}).ObserveReconciler("AnyController", inner)

	require.NotPanics(t, func() {
		_, err := observed.Reconcile(context.Background(), reconcile.Request{})
		require.NoError(t, err)
	})
}
