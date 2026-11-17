ROWS ?= 1000000
RUNS ?= 3
COMPOSE ?= docker compose
EXEC = $(COMPOSE) exec -T

.PHONY: help build up seed bench report all down clean logs bundles

help:
	@echo "make up            build the image and start Airflow + Postgres + S3"
	@echo "make seed ROWS=N   (re)create bench.events with N rows (default $(ROWS))"
	@echo "make bench RUNS=N  trigger N benchmark runs and wait for them"
	@echo "make report        write results/results.csv and results/summary.md"
	@echo "make all           up + seed + bench + report"
	@echo "make down / clean  stop / stop and delete volumes"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d --build --wait

seed:
	$(EXEC) postgres psql -U bench -d source -v rows=$(ROWS) -f /sql/seed.sql

bench:
	$(EXEC) airflow-scheduler bash /opt/airflow/scripts/run_bench.sh $(RUNS)

report:
	$(EXEC) airflow-scheduler python /opt/airflow/scripts/report.py

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
