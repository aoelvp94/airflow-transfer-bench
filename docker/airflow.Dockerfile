# One image for every Airflow component. It carries the three non-Python
# task bundles plus the runtimes their coordinators need (JRE 17, Node.js 22).
# The Go bundle is a static binary and needs no runtime.

ARG AIRFLOW_IMAGE=apache/airflow:3.4.0-python3.12

# ---------- Go bundle ----------
FROM golang:1.25-bookworm AS go-build
WORKDIR /src
COPY impls/go/go.mod impls/go/go.sum ./
RUN go mod download
COPY impls/go/ ./
RUN CGO_ENABLED=0 go tool airflow-go-pack --output /out/transfer_bench_go .

# ---------- Java bundle ----------
FROM maven:3.9-eclipse-temurin-17 AS java-build
WORKDIR /src
COPY impls/java/pom.xml ./
RUN mvn -q -B dependency:go-offline
COPY impls/java/src ./src
RUN mvn -q -B package -DskipTests

# ---------- TypeScript bundle ----------
FROM node:22-bookworm-slim AS ts-build
WORKDIR /src
COPY impls/typescript/package.json impls/typescript/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY impls/typescript/tsconfig.json ./
COPY impls/typescript/src ./src
RUN npm run bundle

# ---------- runtimes ----------
FROM eclipse-temurin:17-jre AS jre
FROM node:22-bookworm-slim AS node

# ---------- Airflow ----------
FROM ${AIRFLOW_IMAGE}
USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --from=node /usr/local/bin/node /usr/local/bin/node
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

# Each bundle gets its own LocalDagBundle directory (see docker-compose.yml).
# `.airflowignore` keeps the Dag processor from parsing these handler-only
# bundles; the coordinators still find them by task_handler_bundle_name.
COPY --from=go-build   /out/transfer_bench_go                  /opt/airflow/bundles/go/transfer_bench_go
COPY --from=java-build /src/target/transfer-bench-java.jar     /opt/airflow/bundles/java/transfer-bench-java.jar
COPY --from=ts-build   /src/dist/transfer_bench_ts.min.mjs     /opt/airflow/bundles/typescript/transfer_bench_ts.min.mjs
RUN for d in go java typescript; do echo '*' > /opt/airflow/bundles/$d/.airflowignore; done \
 && chown -R airflow:root /opt/airflow/bundles

USER airflow
RUN java -version 2>&1 | head -1 && node --version
