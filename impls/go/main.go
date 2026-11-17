// Go implementation of the transfer benchmark task, served through the
// official Airflow Go Task SDK (github.com/apache/airflow/go-sdk).
//
// It answers the `transfer_go` stub in dags/transfer_bench_dag.py.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/url"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/sdk"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
)

// Plan is the `prepare` task's return value. The stub is called as
// transfer_go(plan=...), and a single dict argument decodes whole into this
// struct via its json tags.
type Plan struct {
	Query          string `json:"query"`
	Bucket         string `json:"bucket"`
	KeyPrefix      string `json:"key_prefix"`
	FetchSize      int    `json:"fetch_size"`
	PartSizeMB     int    `json:"part_size_mb"`
	PostgresConnID string `json:"postgres_conn_id"`
	S3ConnID       string `json:"s3_conn_id"`
	// The SDK decodes strictly: every key in the dict needs a field.
	RowCount int64 `json:"row_count"`
}

func transfer(actx airflow.Context, in Plan) (any, error) {
	start := time.Now()
	client := actx.Client()

	pgConn, err := client.GetConnection(actx, in.PostgresConnID)
	if err != nil {
		return nil, fmt.Errorf("postgres connection: %w", err)
	}
	s3Conn, err := client.GetConnection(actx, in.S3ConnID)
	if err != nil {
		return nil, fmt.Errorf("s3 connection: %w", err)
	}

	db, err := pgx.Connect(actx, postgresURL(pgConn))
	if err != nil {
		return nil, err
	}
	defer db.Close(context.Background())

	key := in.KeyPrefix + "/go.csv"
	w, err := newMultipart(actx, newS3(s3Conn), in.Bucket, key, in.PartSizeMB<<20)
	if err != nil {
		return nil, err
	}

	// pgx streams rows off the wire as they arrive; there is no client-side
	// fetch size to tune, so FetchSize is unused here.
	rows, err := db.Query(actx, in.Query)
	if err != nil {
		w.abort(actx)
		return nil, err
	}
	var line bytes.Buffer
	for i, fd := range rows.FieldDescriptions() {
		if i > 0 {
			line.WriteByte(',')
		}
		writeField(&line, []byte(fd.Name))
	}
	line.WriteByte('\n')

	var n int64
	var firstRow time.Duration
	for rows.Next() {
		if n == 0 {
			firstRow = time.Since(start)
		}
		for i, v := range rows.RawValues() { // every column is text; NULL is nil
			if i > 0 {
				line.WriteByte(',')
			}
			writeField(&line, v)
		}
		line.WriteByte('\n')
		n++
		if line.Len() >= 64<<10 {
			if err := w.write(actx, line.Bytes()); err != nil {
				w.abort(actx)
				return nil, err
			}
			line.Reset()
		}
	}
	if err := rows.Err(); err != nil {
		w.abort(actx)
		return nil, err
	}
	if err := w.write(actx, line.Bytes()); err != nil {
		w.abort(actx)
		return nil, err
	}
	if err := w.close(actx); err != nil {
		w.abort(actx)
		return nil, err
	}

	return map[string]any{
		"language":    "go",
		"impl":        "pgx + aws-sdk-go-v2 multipart",
		"runtime":     runtime.Version(),
		"rows":        n,
		"bytes":       w.total,
		"parts":       len(w.parts),
		"key":         key,
		"first_row_s": round(firstRow.Seconds()),
		"duration_s":  round(time.Since(start).Seconds()),
		"peak_rss_mb": peakRSSMB(),
	}, nil
}

// writeField applies the CSV rules shared by every implementation:
// quote when the value contains , " \r or \n; double embedded quotes.
func writeField(b *bytes.Buffer, v []byte) {
	if !bytes.ContainsAny(v, ",\"\r\n") {
		b.Write(v)
		return
	}
	b.WriteByte('"')
	b.Write(bytes.ReplaceAll(v, []byte{'"'}, []byte{'"', '"'}))
	b.WriteByte('"')
}

type multipart struct {
	s3       *s3.Client
	bucket   string
	key      string
	uploadID *string
	partSize int
	buf      []byte
	parts    []types.CompletedPart
	total    int64
}

func newMultipart(ctx context.Context, c *s3.Client, bucket, key string, partSize int) (*multipart, error) {
	out, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	return &multipart{s3: c, bucket: bucket, key: key, uploadID: out.UploadId, partSize: partSize,
		buf: make([]byte, 0, partSize+(1<<20))}, nil
}

func (m *multipart) write(ctx context.Context, p []byte) error {
	m.buf = append(m.buf, p...)
	m.total += int64(len(p))
	if len(m.buf) >= m.partSize {
		return m.flush(ctx)
	}
	return nil
}

func (m *multipart) flush(ctx context.Context) error {
	num := int32(len(m.parts) + 1)
	out, err := m.s3.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: &m.bucket, Key: &m.key, UploadId: m.uploadID, PartNumber: &num,
		Body: bytes.NewReader(m.buf),
	})
	if err != nil {
		return err
	}
	m.parts = append(m.parts, types.CompletedPart{ETag: out.ETag, PartNumber: &num})
	m.buf = m.buf[:0]
	return nil
}

func (m *multipart) close(ctx context.Context) error {
	if len(m.buf) > 0 || len(m.parts) == 0 {
		if err := m.flush(ctx); err != nil {
			return err
		}
	}
	_, err := m.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: &m.bucket, Key: &m.key, UploadId: m.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: m.parts},
	})
	return err
}

func (m *multipart) abort(ctx context.Context) {
	_, _ = m.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: &m.bucket, Key: &m.key, UploadId: m.uploadID,
	})
}

func postgresURL(c sdk.Connection) string {
	u := url.URL{Scheme: "postgres", Host: c.Host + ":" + strconv.Itoa(c.Port), Path: "/" + c.Path}
	if c.Login != nil {
		if c.Password != nil {
			u.User = url.UserPassword(*c.Login, *c.Password)
		} else {
			u.User = url.User(*c.Login)
		}
	}
	return u.String()
}

func newS3(c sdk.Connection) *s3.Client {
	str := func(k, def string) string {
		if v, ok := c.Extra[k].(string); ok && v != "" {
			return v
		}
		return def
	}
	var login, password string
	if c.Login != nil {
		login = *c.Login
	}
	if c.Password != nil {
		password = *c.Password
	}
	opts := s3.Options{
		Region:                     str("region_name", "us-east-1"),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider(login, password, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	}
	if ep := str("endpoint_url", ""); ep != "" {
		opts.BaseEndpoint = aws.String(ep)
	}
	return s3.New(opts)
}

func peakRSSMB() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return round(float64(ru.Maxrss) / 1024) // KiB on Linux
}

func round(f float64) float64 { return float64(int64(f*10000+0.5)) / 10000 }

func main() {
	bundle := airflow.Bundle()
	bundle.Register(airflow.TaskHandler("transfer_bench", "transfer_go", transfer))
	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
