ROWS ?= 1000000
# 5 triggered runs. report.py discards the earliest as a warm-up.
RUNS ?= 5
COMPOSE ?= docker compose
EXEC = $(COMPOSE) exec -T

AIRFLOW_VERSION ?= 3.4.0
AIRFLOW_PYTHON ?= 3.12
GO_SDK_VERSION ?= 1.0.0
JAVA_SDK_VERSION ?= 1.0.0
TS_SDK_VERSION ?= 1.0.0
SUPERVISOR_SCHEMA_VERSION ?= 2026-10-30
GO_VERSION ?= 1.25
JAVA_MAJOR ?= 17
NODE_MAJOR ?= 22
BENCH_CPUS ?= 2
BENCH_MEM ?= 6g

export AIRFLOW_VERSION AIRFLOW_PYTHON GO_SDK_VERSION JAVA_SDK_VERSION TS_SDK_VERSION
export SUPERVISOR_SCHEMA_VERSION GO_VERSION JAVA_MAJOR NODE_MAJOR
export BENCH_CPUS BENCH_MEM

GIT_SHA := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
# results/ is what report writes. A dirty results tree is not a dirty bench.
GIT_DIRTY := $(shell git status --porcelain -- . ':(exclude)results' 2>/dev/null | grep -q . && echo true || echo false)

REPORT_ENV = -e BENCH_GIT_SHA="$(GIT_SHA)" -e BENCH_GIT_DIRTY="$(GIT_DIRTY)" -e BENCH_LATEST_RUNS="$(RUNS)"
ifneq ($(BENCH_ENV_SLUG),)
REPORT_ENV += -e BENCH_ENV_SLUG="$(BENCH_ENV_SLUG)"
endif
ifneq ($(BENCH_CI),)
REPORT_ENV += -e BENCH_CI="$(BENCH_CI)"
endif

.PHONY: help build up seed bench report index all down clean logs bundles

help:
	@echo "make up                         build the image and start Airflow + Postgres + S3"
	@echo "    AIRFLOW_VERSION=$(AIRFLOW_VERSION) GO_SDK_VERSION=$(GO_SDK_VERSION)"
	@echo "    JAVA_SDK_VERSION=$(JAVA_SDK_VERSION) TS_SDK_VERSION=$(TS_SDK_VERSION)"
	@echo "make seed ROWS=N                (re)create bench.events (default $(ROWS))"
	@echo "make bench RUNS=N               trigger N runs and wait (default $(RUNS); first is warm-up)"
	@echo "make report                     write gitignored results/ and prepend RESULTS.md"
	@echo "make index                      re-render RESULTS.md from the entries already in it"
	@echo "make all                        up + seed + bench + report"
	@echo "make down / clean               stop / stop and delete volumes"
	@echo "BENCH_CPUS=$(BENCH_CPUS) BENCH_MEM=$(BENCH_MEM)   scheduler container limits"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d --build --wait --wait-timeout 600

seed:
	$(EXEC) postgres psql -U bench -d source -v rows=$(ROWS) -f /sql/seed.sql

bench:
	$(EXEC) airflow-scheduler bash /opt/airflow/scripts/run_bench.sh $(RUNS)

report:
	chmod a+rw RESULTS.md
	$(EXEC) $(REPORT_ENV) airflow-scheduler python /opt/airflow/scripts/report.py

index:
	python3 scripts/report.py --index-only

all: up seed bench report

logs:
	$(COMPOSE) logs -f airflow-scheduler

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

# Build the three non-Python bundles on the host (needs Go 1.25+, Maven/JDK 17+, Node 22+).
bundles:
	cd impls/go && go tool airflow-go-pack --output dist/transfer_bench_go .
	cd impls/java && mvn -q -B package -DskipTests
	cd impls/typescript && npm ci && npm run bundle
