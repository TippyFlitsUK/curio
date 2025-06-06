package pdp

import (
	"context"
	"net"
	"net/http"
	"net/url"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// Tag keys for retrieval metrics
	serviceTagKey, _    = tag.NewKey("service")
	domainTagKey, _     = tag.NewKey("domain")
	statusCodeTagKey, _ = tag.NewKey("status_code")
	pieceSizeTagKey, _  = tag.NewKey("piece_size_category")

	pre = "curio_pdp_"

	// Duration buckets for response times (in milliseconds)
	durationBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}

	// Size buckets for piece sizes (in MB)
	sizeBuckets = []float64{0.1, 1, 10, 50, 100, 500, 1000, 5000, 10000}
)

var (
	// Measures for PDP piece access tracking (when PDP pieces are retrieved via /piece/{cid})
	PDPPieceAccess       = stats.Int64(pre+"piece_access", "Number of times PDP pieces are accessed via main retrieval endpoint", stats.UnitDimensionless)
	PDPPieceAccessDomain = stats.Int64(pre+"piece_access_by_domain", "Number of PDP piece accesses by domain", stats.UnitDimensionless)
	PDPPieceBytesServed  = stats.Int64(pre+"piece_bytes_served", "Total bytes served for PDP pieces", stats.UnitBytes)
	PDPPieceSize         = stats.Int64(pre+"piece_size", "Size distribution of PDP pieces accessed", stats.UnitBytes)
)

func init() {
	err := view.Register(
		&view.View{
			Measure:     PDPPieceAccess,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{},
		},
		&view.View{
			Measure:     PDPPieceAccessDomain,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{domainTagKey},
		},
		&view.View{
			Measure:     PDPPieceBytesServed,
			Aggregation: view.Sum(),
			TagKeys:     []tag.Key{domainTagKey},
		},
		&view.View{
			Measure:     PDPPieceSize,
			Aggregation: view.Distribution(sizeBuckets...),
			TagKeys:     []tag.Key{pieceSizeTagKey},
		},
	)
	if err != nil {
		panic(err)
	}
}

// categorizeSize returns a category string for the given size in bytes
func categorizeSize(sizeBytes int64) string {
	sizeMB := float64(sizeBytes) / (1024 * 1024)

	switch {
	case sizeMB < 1:
		return "small"
	case sizeMB < 100:
		return "medium"
	case sizeMB < 1000:
		return "large"
	default:
		return "xlarge"
	}
}

// extractDomain extracts the domain from the request for metrics
func extractDomain(r *http.Request) string {
	// Try to get domain from Referer header first
	if referer := r.Header.Get("Referer"); referer != "" {
		if u, err := url.Parse(referer); err == nil && u.Host != "" {
			return u.Host
		}
	}

	// Try to get domain from Host header
	if host := r.Header.Get("Host"); host != "" {
		return host
	}

	// Try to get from X-Forwarded-Host header
	if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		return forwardedHost
	}

	// Fall back to remote address
	if remoteAddr := r.RemoteAddr; remoteAddr != "" {
		if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
			return host
		}
		return remoteAddr
	}

	return "unknown"
}

// RecordPDPPieceAccess records metrics when a PDP piece is accessed via the main retrieval endpoint
// This function should be called from the retrieval system when serving PDP pieces
func RecordPDPPieceAccess(ctx context.Context, r *http.Request, pieceSize int64) {
	domain := extractDomain(r)
	sizeCategory := categorizeSize(pieceSize)

	// Record basic access
	stats.Record(ctx, PDPPieceAccess.M(1))

	// Record access by domain
	if domainCtx, err := tag.New(ctx, tag.Insert(domainTagKey, domain)); err == nil {
		stats.Record(domainCtx, PDPPieceAccessDomain.M(1))
		stats.Record(domainCtx, PDPPieceBytesServed.M(pieceSize))
	}

	// Record piece size distribution
	if sizeCtx, err := tag.New(ctx, tag.Insert(pieceSizeTagKey, sizeCategory)); err == nil {
		stats.Record(sizeCtx, PDPPieceSize.M(pieceSize))
	}
}

// IsPDPPiece checks if a piece CID corresponds to a PDP piece by checking the database
// This function can be used by the retrieval system to identify PDP pieces
func IsPDPPiece(ctx context.Context, db interface{}, pieceCID string) bool {
	// Define interface matching harmonydb.DB
	type Row interface {
		Scan(dest ...interface{}) error
	}
	type querier interface {
		QueryRow(ctx context.Context, sql string, args ...interface{}) Row
	}

	q, ok := db.(querier)
	if !ok {
		return false
	}

	var count int
	err := q.QueryRow(ctx, `
		SELECT 1 FROM parked_pieces pp
		JOIN parked_piece_refs ppr ON ppr.piece_id = pp.id
		JOIN pdp_piecerefs pdpr ON pdpr.piece_ref = ppr.ref_id
		WHERE pp.piece_cid = $1 AND pp.complete = TRUE
		LIMIT 1
	`, pieceCID).Scan(&count)

	return err == nil && count == 1
}
