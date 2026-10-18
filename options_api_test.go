package cacheq_test

import (
	"context"

	"github.com/Waterkyuu/cacheq"
)

// These assignments require exactly one configuration argument at compile time.
// A variadic signature would permit silently discarded options and cannot satisfy these types.
var _ func(*cacheq.Client, any, cacheq.Fetcher[int], cacheq.QueryOptions) *cacheq.QueryHandle[int] = cacheq.Query[int]

var _ func(any, cacheq.Fetcher[int], cacheq.QueryOptions) cacheq.Definition[int] = cacheq.QueryDefinition[int]

var _ func(
	context.Context,
	*cacheq.Client,
	any,
	cacheq.Fetcher[int],
	cacheq.QueryOptions,
) (int, error) = cacheq.Fetch[int]

var _ func(context.Context, *cacheq.Client, any, cacheq.Fetcher[int], cacheq.QueryOptions) error = cacheq.Prefetch[int]

var _ func(
	*cacheq.Client,
	cacheq.Mutator[int, string],
	func(context.Context, string, int) error,
	cacheq.MutationOptions,
) *cacheq.MutationHandle[int, string] = cacheq.Mutation[int, string]

var _ func(*cacheq.Client, any, cacheq.InvalidateOptions) error = (*cacheq.Client).Invalidate

var _ func(
	*cacheq.Client,
	context.Context,
	any,
	cacheq.InvalidateOptions,
) ([]cacheq.RefreshResult, error) = (*cacheq.Client).InvalidateAndWait

var _ func(
	cacheq.Definition[int],
	context.Context,
	*cacheq.Client,
	cacheq.InvalidateOptions,
) error = cacheq.Definition[int].InvalidateShared

var _ func(
	*cacheq.Client,
	context.Context,
	cacheq.SharedInvalidationStore,
	cacheq.InvalidateOptions,
) (*cacheq.SharedWatcher, error) = (*cacheq.Client).WatchShared

var _ func(
	*cacheq.Client,
	context.Context,
	cacheq.SharedTagStore,
	[]string,
	cacheq.InvalidateOptions,
) error = (*cacheq.Client).InvalidateSharedTags
