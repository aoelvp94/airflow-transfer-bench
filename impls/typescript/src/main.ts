// TypeScript implementation of the `transfer_typescript` stub task, served
// through the official Airflow TypeScript Task SDK (@apache-airflow/ts-sdk).
import { Bundle, TaskHandler, getClient } from "@apache-airflow/ts-sdk";
import type { ConnectionResult } from "@apache-airflow/ts-sdk";
import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  S3Client,
  UploadPartCommand,
  type CompletedPart,
} from "@aws-sdk/client-s3";
import postgres from "postgres";

/** The `prepare` task's return value; keys arrive exactly as Python wrote them. */
interface Plan {
  query: string;
  bucket: string;
  key_prefix: string;
  fetch_size: number;
  part_size_mb: number;
  postgres_conn_id: string;
  s3_conn_id: string;
}

/** The stub is called as transfer_typescript(plan=...). */
interface TransferArgs {
  plan: Plan;
}

const NEEDS_QUOTE = /[",\r\n]/;

/** Shared CSV rule: quote when the value contains , " \r or \n. NULL is empty. */
function field(v: string | null): string {
  if (v === null || v === undefined) return "";
  return NEEDS_QUOTE.test(v) ? `"${v.replaceAll('"', '""')}"` : v;
}

class Multipart {
  parts: CompletedPart[] = [];
  total = 0;
  private chunks: Buffer[] = [];
  private buffered = 0;

  private constructor(
    private s3: S3Client,
    private bucket: string,
    private key: string,
    private uploadId: string,
    private partSize: number,
  ) {}

  static async open(s3: S3Client, bucket: string, key: string, partSize: number) {
    const out = await s3.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key }));
    return new Multipart(s3, bucket, key, out.UploadId!, partSize);
  }

  async write(data: Buffer) {
    this.chunks.push(data);
    this.buffered += data.length;
    this.total += data.length;
    if (this.buffered >= this.partSize) await this.flush();
  }

  private async flush() {
    const PartNumber = this.parts.length + 1;
    const out = await this.s3.send(
      new UploadPartCommand({
        Bucket: this.bucket, Key: this.key, UploadId: this.uploadId, PartNumber,
        Body: Buffer.concat(this.chunks, this.buffered),
      }),
    );
    this.parts.push({ ETag: out.ETag, PartNumber });
    this.chunks = [];
    this.buffered = 0;
  }

  async close() {
    if (this.buffered > 0 || this.parts.length === 0) await this.flush();
    await this.s3.send(
      new CompleteMultipartUploadCommand({
        Bucket: this.bucket, Key: this.key, UploadId: this.uploadId,
        MultipartUpload: { Parts: this.parts },
      }),
    );
  }

  async abort() {
    await this.s3
      .send(new AbortMultipartUploadCommand({ Bucket: this.bucket, Key: this.key, UploadId: this.uploadId }))
      .catch(() => undefined);
  }
}

function s3Client(c: ConnectionResult): S3Client {
  const extra = c.extra ? (JSON.parse(c.extra) as Record<string, string>) : {};
  return new S3Client({
    region: extra.region_name ?? "us-east-1",
    endpoint: extra.endpoint_url || undefined,
    forcePathStyle: true,
    credentials: { accessKeyId: c.login ?? "", secretAccessKey: c.password ?? "" },
    requestChecksumCalculation: "WHEN_REQUIRED",
  });
}

const round = (n: number) => Math.round(n * 10000) / 10000;

async function transfer({ plan: args }: TransferArgs) {
  const start = performance.now();
  const cpuStart = process.cpuUsage();
  const client = getClient();
  const pg = await client.getConnectionOrThrow(args.postgres_conn_id);
  const s3c = await client.getConnectionOrThrow(args.s3_conn_id);

  const sql = postgres({
    host: pg.host ?? undefined,
    port: pg.port ?? 5432,
    database: pg.schema ?? undefined,
    username: pg.login ?? undefined,
    password: pg.password ?? undefined,
    max: 1,
  });
  const key = `${args.key_prefix}/typescript.csv`;
  const w = await Multipart.open(s3Client(s3c), args.bucket, key, args.part_size_mb * 1024 * 1024);
  let rows = 0;
  let firstRow = 0;
  try {
    let header = true;
    // Server-side cursor: `fetchSize` rows per round trip.
    for await (const batch of sql.unsafe(args.query).cursor(args.fetch_size)) {
      if (rows === 0) firstRow = (performance.now() - start) / 1000;
      let out = "";
      if (header && batch.length > 0) {
        // Cursor batches carry no column metadata; row objects keep column order.
        out += Object.keys(batch[0]).map(field).join(",") + "\n";
        header = false;
      }
      for (const row of batch) {
        out += Object.values(row).map((v) => field(v as string | null)).join(",") + "\n";
      }
      rows += batch.length;
      await w.write(Buffer.from(out, "utf8"));
    }
    await w.close();
  } catch (err) {
    await w.abort();
    throw err;
  } finally {
    await sql.end();
  }

  const cpu = process.cpuUsage(cpuStart);
  return {
    language: "typescript",
    impl: "postgres.js cursor + AWS SDK v3 multipart",
    runtime: `Node.js ${process.version}`,
    rows,
    bytes: w.total,
    parts: w.parts.length,
    key,
    first_row_s: round(firstRow),
    duration_s: round((performance.now() - start) / 1000),
    // Process CPU over the same window as duration_s. cpuUsage is microseconds.
    cpu_s: round((cpu.user + cpu.system) / 1e6),
    peak_rss_mb: round(process.resourceUsage().maxRSS / 1024), // KiB -> MiB
  };
}

await new Bundle(new TaskHandler("transfer_bench", "transfer_typescript", transfer)).serve();
