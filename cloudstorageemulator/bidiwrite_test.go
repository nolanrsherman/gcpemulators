package cloudstorageemulator

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/googleapis/gax-go/v2"
)

type gRPCWriteRequestParams struct {
	appendable   bool
	bucket       string
	routingToken *string
}

func (p gRPCWriteRequestParams) apply(ctx context.Context) context.Context {
	hds := make([]string, 0, 3)
	if p.appendable {
		hds = append(hds, "appendable=true")
	}
	if p.bucket != "" {
		hds = append(hds, fmt.Sprintf("bucket=projects/_/buckets/%s", url.QueryEscape(p.bucket)))
	}
	if p.routingToken != nil {
		hds = append(hds, fmt.Sprintf("routing_token=%s", *p.routingToken))
	}
	return gax.InsertMetadataIntoOutgoingContext(ctx, "x-goog-request-params", strings.Join(hds, "&"))
}
