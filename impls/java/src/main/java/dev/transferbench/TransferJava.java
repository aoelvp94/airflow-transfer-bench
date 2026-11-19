package dev.transferbench;

import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.InputTask;
import org.apache.airflow.sdk.TaskInput;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/** Java implementation of the {@code transfer_java} stub task. */
public final class TransferJava implements InputTask<TransferJava.Input> {

  /** The stub is called as transfer_java(plan=...): one keyword, the `prepare` task's dict. */
  public static class Input implements TaskInput {
    public Map<String, Object> plan;
  }

  /** Typed view of the plan dict. */
  record Plan(String query, String bucket, String keyPrefix, int fetchSize, int partSizeMb,
              String postgresConnId, String s3ConnId) {
    static Plan of(Map<String, Object> m) {
      return new Plan((String) m.get("query"), (String) m.get("bucket"), (String) m.get("key_prefix"),
          ((Number) m.get("fetch_size")).intValue(), ((Number) m.get("part_size_mb")).intValue(),
          (String) m.get("postgres_conn_id"), (String) m.get("s3_conn_id"));
    }
  }

  @Override
  public void execute(Context context, Client client, Input input) throws Exception {
    Plan in = Plan.of(input.plan);
    long start = System.nanoTime();
    long cpuStart = processCpuNanos();
    org.apache.airflow.sdk.Connection pg = client.getConnection(in.postgresConnId());
    org.apache.airflow.sdk.Connection s3c = client.getConnection(in.s3ConnId());

    String key = in.keyPrefix() + "/java.csv";
    Multipart w = new Multipart(s3(s3c), in.bucket(), key, in.partSizeMb() * 1024 * 1024);
    long rows = 0;
    double firstRow = 0;
    String jdbcUrl = "jdbc:postgresql://" + pg.host + ":" + pg.port + "/" + pg.schema;
    try (Connection db = DriverManager.getConnection(jdbcUrl, pg.login, pg.password)) {
      db.setAutoCommit(false); // required for a server-side cursor in pgjdbc
      try (PreparedStatement st = db.prepareStatement(in.query())) {
        st.setFetchSize(in.fetchSize());
        try (ResultSet rs = st.executeQuery()) {
          ResultSetMetaData md = rs.getMetaData();
          int cols = md.getColumnCount();
          StringBuilder line = new StringBuilder(1 << 16);
          for (int i = 1; i <= cols; i++) {
            if (i > 1) line.append(',');
            appendField(line, md.getColumnLabel(i));
          }
          line.append('\n');
          while (rs.next()) {
            if (rows == 0) firstRow = seconds(start);
            for (int i = 1; i <= cols; i++) {
              if (i > 1) line.append(',');
              appendField(line, rs.getString(i));
            }
            line.append('\n');
            rows++;
            if (line.length() >= 1 << 16) {
              w.write(line.toString().getBytes(StandardCharsets.UTF_8));
              line.setLength(0);
            }
          }
          w.write(line.toString().getBytes(StandardCharsets.UTF_8));
        }
      }
      w.close();
    } catch (Exception e) {
      w.abort();
      throw e;
    }

    Map<String, Object> metrics = new LinkedHashMap<>();
    metrics.put("language", "java");
    metrics.put("impl", "pgjdbc fetchSize + AWS SDK v2 multipart");
    metrics.put("runtime", "Java " + System.getProperty("java.version"));
    metrics.put("rows", rows);
    metrics.put("bytes", w.total);
    metrics.put("parts", w.parts.size());
    metrics.put("key", key);
    metrics.put("first_row_s", round(firstRow));
    metrics.put("duration_s", round(seconds(start)));
    long cpuEnd = processCpuNanos();
    if (cpuStart >= 0 && cpuEnd >= cpuStart) {
      // Process CPU over the same window as duration_s (the work), not task startup.
      metrics.put("cpu_s", round((cpuEnd - cpuStart) / 1e9));
    }
    metrics.put("peak_rss_mb", peakRssMb());
    client.setXCom(metrics);
  }

  /** Shared CSV rule: quote when the value contains , " \r or \n. NULL is empty. */
  static void appendField(StringBuilder b, String v) {
    if (v == null) return;
    boolean quote = false;
    for (int i = 0; i < v.length(); i++) {
      char c = v.charAt(i);
      if (c == ',' || c == '"' || c == '\r' || c == '\n') { quote = true; break; }
    }
    if (!quote) { b.append(v); return; }
    b.append('"').append(v.replace("\"", "\"\"")).append('"');
  }

  @SuppressWarnings("unchecked")
  static S3Client s3(org.apache.airflow.sdk.Connection c) throws Exception {
    Map<String, Object> extra;
    if (c.extra instanceof Map) extra = (Map<String, Object>) c.extra;
    else if (c.extra instanceof String s && !s.isBlank()) extra = new ObjectMapper().readValue(s, Map.class);
    else extra = Map.of();
    S3ClientBuilder b = S3Client.builder()
        .region(Region.of((String) extra.getOrDefault("region_name", "us-east-1")))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(c.login, c.password)))
        .forcePathStyle(true)
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .httpClient(UrlConnectionHttpClient.create());
    Object endpoint = extra.get("endpoint_url");
    if (endpoint instanceof String e && !e.isBlank()) b.endpointOverride(URI.create(e));
    return b.build();
  }

  static final class Multipart {
    final S3Client s3;
    final String bucket, key, uploadId;
    final int partSize;
    final ByteArrayOutputStream buf;
    final List<CompletedPart> parts = new ArrayList<>();
    long total;

    Multipart(S3Client s3, String bucket, String key, int partSize) {
      this.s3 = s3;
      this.bucket = bucket;
      this.key = key;
      this.partSize = partSize;
      this.buf = new ByteArrayOutputStream(partSize + (1 << 20));
      this.uploadId = s3.createMultipartUpload(r -> r.bucket(bucket).key(key)).uploadId();
    }

    void write(byte[] p) {
      buf.write(p, 0, p.length);
      total += p.length;
      if (buf.size() >= partSize) flush();
    }

    void flush() {
      int n = parts.size() + 1;
      String etag = s3.uploadPart(r -> r.bucket(bucket).key(key).uploadId(uploadId).partNumber(n),
          RequestBody.fromBytes(buf.toByteArray())).eTag();
      parts.add(CompletedPart.builder().partNumber(n).eTag(etag).build());
      buf.reset();
    }

    void close() {
      if (buf.size() > 0 || parts.isEmpty()) flush();
      s3.completeMultipartUpload(r -> r.bucket(bucket).key(key).uploadId(uploadId)
          .multipartUpload(m -> m.parts(parts)));
    }

    void abort() {
      try {
        s3.abortMultipartUpload(r -> r.bucket(bucket).key(key).uploadId(uploadId));
      } catch (Exception ignored) {
        // best effort
      }
    }
  }

  /**
   * Process CPU time in nanoseconds (user + system), or -1 when it cannot be read.
   * The HotSpot counter is preferred. Reflection goes through the exported
   * {@code com.sun.management.OperatingSystemMXBean} interface: resolving
   * {@code getProcessCpuTime} on the concrete class fails, because that class
   * lives in the unexported {@code com.sun.management.internal} package.
   * On Linux the fallback is utime+stime from {@code /proc/self/stat}
   * (USER_HZ is 100, so each tick is 10 ms).
   */
  static long processCpuNanos() {
    long fromBean = hotspotProcessCpuNanos();
    if (fromBean >= 0) return fromBean;
    return linuxProcCpuNanos();
  }

  static long hotspotProcessCpuNanos() {
    try {
      Object os = ManagementFactory.getOperatingSystemMXBean();
      Class<?> type = Class.forName("com.sun.management.OperatingSystemMXBean");
      if (!type.isInstance(os)) return -1L;
      Object nanos = type.getMethod("getProcessCpuTime").invoke(os);
      if (nanos instanceof Long value && value >= 0) return value;
    } catch (Exception ignored) {
      // not a HotSpot JVM, or jdk.management is not resolvable
    }
    return -1L;
  }

  static long linuxProcCpuNanos() {
    try {
      String stat = Files.readString(Path.of("/proc/self/stat"));
      int end = stat.lastIndexOf(')');
      if (end < 0 || end + 2 >= stat.length()) return -1L;
      String[] fields = stat.substring(end + 2).trim().split("\\s+");
      // After comm: state, ppid, pgrp, session, tty, tpgid, flags,
      // minflt, cminflt, majflt, cmajflt, utime, stime.
      if (fields.length < 13) return -1L;
      long ticks = Long.parseLong(fields[11]) + Long.parseLong(fields[12]);
      return ticks * 10_000_000L;
    } catch (Exception ignored) {
      return -1L;
    }
  }

  static double peakRssMb() {
    try {
      for (String l : Files.readAllLines(Path.of("/proc/self/status"))) {
        if (l.startsWith("VmHWM:")) {
          return round(Long.parseLong(l.replaceAll("[^0-9]", "")) / 1024.0);
        }
      }
    } catch (Exception ignored) {
      // not Linux
    }
    return 0;
  }

  static double seconds(long startNanos) {
    return (System.nanoTime() - startNanos) / 1e9;
  }

  static double round(double d) {
    return Math.round(d * 10000) / 10000.0;
  }
}
