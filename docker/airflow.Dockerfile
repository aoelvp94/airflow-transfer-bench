# One image for every Airflow component. It carries the three non-Python
# task bundles plus the runtimes their coordinators need (JRE 17, Node.js 22).
# The Go bundle is a static binary and needs no runtime.
#
# Build args default to Airflow 3.4.0 and Task SDKs 1.0.0. They are versions,
# not credentials. docker-compose.yml forwards the same names from the
# environment (see the Makefile).

ARG AIRFLOW_VERSION=3.4.0
ARG AIRFLOW_PYTHON=3.12
ARG GO_VERSION=1.25
ARG JAVA_MAJOR=17
ARG NODE_MAJOR=22

# ---------- Go bundle ----------
FROM golang:${GO_VERSION}-bookworm AS go-build
ARG GO_SDK_VERSION=1.0.0
WORKDIR /src
COPY impls/go/go.mod impls/go/go.sum ./
RUN go mod download
COPY impls/go/ ./
# The committed go.sum is the default. Another GO_SDK_VERSION re-resolves the module.
# No shell variables here: BuildKit would expand them. The pin is compared as two files.
RUN sed -n 's/.*github.com\/apache\/airflow\/go-sdk v//p' go.mod | head -n 1 > /tmp/current-go-sdk \
 && printf '%s\n' "${GO_SDK_VERSION}" > /tmp/wanted-go-sdk \
 && if cmp -s /tmp/current-go-sdk /tmp/wanted-go-sdk; then \
      echo "go-sdk ${GO_SDK_VERSION} matches go.mod"; \
    else \
      go get "github.com/apache/airflow/go-sdk@v${GO_SDK_VERSION}"; \
      go mod tidy; \
    fi \
 && CGO_ENABLED=0 go tool airflow-go-pack --output /out/transfer_bench_go .

# ---------- Java bundle ----------
FROM maven:3.9-eclipse-temurin-${JAVA_MAJOR} AS java-build
ARG JAVA_SDK_VERSION=1.0.0
ARG SUPERVISOR_SCHEMA_VERSION=2026-10-30
WORKDIR /src
COPY impls/java/pom.xml ./
RUN mvn -q -B dependency:go-offline \
      -Dairflow.sdk.version="${JAVA_SDK_VERSION}" \
      -Dairflow.supervisor.schema.version="${SUPERVISOR_SCHEMA_VERSION}"
COPY impls/java/src ./src
RUN mvn -q -B package -DskipTests \
      -Dairflow.sdk.version="${JAVA_SDK_VERSION}" \
      -Dairflow.supervisor.schema.version="${SUPERVISOR_SCHEMA_VERSION}"

# ---------- TypeScript bundle ----------
FROM node:${NODE_MAJOR}-bookworm-slim AS ts-build
ARG TS_SDK_VERSION=1.0.0
WORKDIR /src
COPY impls/typescript/package.json impls/typescript/package-lock.json ./
RUN npm ci --no-audit --no-fund
# Re-pin only when the requested version is not the one npm ci just installed.
RUN node -e 'const v=require("./node_modules/@apache-airflow/ts-sdk/package.json").version; if (v !== process.env.TS_SDK_VERSION) process.exit(42)' \
 || npm install --no-audit --no-fund --save-exact "@apache-airflow/ts-sdk@${TS_SDK_VERSION}"
COPY impls/typescript/tsconfig.json ./
COPY impls/typescript/src ./src
RUN npm run bundle

# ---------- runtimes ----------
FROM eclipse-temurin:${JAVA_MAJOR}-jre AS jre
FROM node:${NODE_MAJOR}-bookworm-slim AS node

# ---------- Airflow ----------
FROM apache/airflow:${AIRFLOW_VERSION}-python${AIRFLOW_PYTHON}

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

ARG AIRFLOW_VERSION=3.4.0
ARG AIRFLOW_PYTHON=3.12
ARG GO_SDK_VERSION=1.0.0
ARG JAVA_SDK_VERSION=1.0.0
ARG TS_SDK_VERSION=1.0.0
ARG SUPERVISOR_SCHEMA_VERSION=2026-10-30
ARG GO_VERSION=1.25
ARG JAVA_MAJOR=17
ARG NODE_MAJOR=22
RUN printf '*\n' > /opt/airflow/bundles/go/.airflowignore \
 && printf '*\n' > /opt/airflow/bundles/java/.airflowignore \
 && printf '*\n' > /opt/airflow/bundles/typescript/.airflowignore \
 && python -c 'import json, os; from pathlib import Path; Path("/opt/airflow/bench-build.json").write_text(json.dumps({"airflow_version": os.environ["AIRFLOW_VERSION"], "airflow_python": os.environ["AIRFLOW_PYTHON"], "sdk": {"go": os.environ["GO_SDK_VERSION"], "java": os.environ["JAVA_SDK_VERSION"], "typescript": os.environ["TS_SDK_VERSION"]}, "toolchains": {"go_image": "golang:" + os.environ["GO_VERSION"] + "-bookworm", "jre_image": "eclipse-temurin:" + os.environ["JAVA_MAJOR"] + "-jre", "node_image": "node:" + os.environ["NODE_MAJOR"] + "-bookworm-slim"}, "supervisor_schema_version": os.environ["SUPERVISOR_SCHEMA_VERSION"]}, indent=2) + "\n")' \
 && chown -R airflow:root /opt/airflow/bundles /opt/airflow/bench-build.json

USER airflow
# The stock apache/airflow image does not ship providers. Pin them to this
# release's constraints so PostgresHook and SqlToS3Operator match the Airflow
# version the image was built from.
RUN pip install --no-cache-dir \
      "apache-airflow-providers-amazon" \
      "apache-airflow-providers-postgres" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON}.txt" \
 && java -version \
 && node --version
