.PHONY: prepare install update lint check test test-pg test-pipeline test-graph test-enrichment test-api serve test-py test-sq test-matrix-py test-matrix-sq

prepare: lint check

install:
	uv sync

update:
	uv run scripts/update-gql-ops.py
	uv run scripts/update-mocked-data.py refresh

update-deps:
	uv sync --upgrade --all-groups
	uv --preview-features audit audit

lint:
	uv run ruff check --select I --fix .
	uv run ruff format .

check:
	uv run ruff format --check .
	uv run ruff check .
	uv run ty check

test:
	@uv run pytest -s --cov=twscrape --cov=xgraph tests/

# The PostgreSQL contracts (frontier claim, account leasing, schema constraints)
# are skipped unless XGRAPH_TEST_DATABASE_URL points at a real server, so they
# have to run as their own gate. Requires PostgreSQL 15+ for NULLS NOT DISTINCT.
test-pg:
	@XGRAPH_TEST_DATABASE_URL=$${XGRAPH_TEST_DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/xgraph_test} \
		uv run pytest -q tests/xgraph/test_postgres_integration.py

# The stage 3 delivery guarantees are properties of the boundary between
# PostgreSQL and the broker, so neither half can be faked. Without KAFKA the
# broker round-trip is skipped and only the transactional contracts run.
test-pipeline:
	@XGRAPH_TEST_DATABASE_URL=$${XGRAPH_TEST_DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/xgraph_test} \
	 XGRAPH_TEST_KAFKA_BOOTSTRAP=$${XGRAPH_TEST_KAFKA_BOOTSTRAP:-127.0.0.1:9092} \
		uv run pytest -q tests/xgraph/test_phase3_integration.py

# The L0-L6 traversal is enforced by constraints and set-based statements that
# a fake cannot reproduce: node uniqueness, depth assignment and layer closure
# are all properties of the database.
test-graph:
	@XGRAPH_TEST_DATABASE_URL=$${XGRAPH_TEST_DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/xgraph_test} \
		uv run pytest -q tests/xgraph/test_phase4_integration.py

# Enrichment must stay isolated from the traversal: its own rate-limit bucket,
# its own frontier rows, no ability to hold a layer open. All three are database
# properties.
test-enrichment:
	@XGRAPH_TEST_DATABASE_URL=$${XGRAPH_TEST_DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/xgraph_test} \
		uv run pytest -q tests/xgraph/test_phase5_integration.py

# The product surface is asserted against the same fact store the pipeline
# writes, including the rule that no route may expose scraper identities.
test-api:
	@XGRAPH_TEST_DATABASE_URL=$${XGRAPH_TEST_DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/xgraph_test} \
		uv run pytest -q tests/xgraph/test_phase6_integration.py

serve:
	@XGRAPH_DATABASE_URL=$${XGRAPH_DATABASE_URL:?set XGRAPH_DATABASE_URL} \
		uv run python -m xgraph.api

test-py:
	$(eval name=twscrape_py$(v))
	@docker -l warning build -f Dockerfile.py-matrix --build-arg VER=$(v) -t $(name) .
	@docker run $(name)

test-sq:
	$(eval name=twscrape_sq$(v))
	@docker -l warning build -f Dockerfile.sq-matrix --build-arg SQLY=$(y) --build-arg SQLV=$(v) -t $(name) .
	@docker run $(name)

test-matrix-py:
	@make test-py v=3.10
	@make test-py v=3.11
	@make test-py v=3.12
	@make test-py v=3.13
	@make test-py v=3.14

test-matrix-sq:
	@# https://www.sqlite.org/chronology.html https://www.sqlite.org/download.html
	@make test-sq y=2018 v=3240000
# 	@make test-sq y=2019 v=3270200
# 	@make test-sq y=2019 v=3300100
# 	@make test-sq y=2020 v=3330000
# 	@make test-sq y=2021 v=3340100
# 	@make test-sq y=2023 v=3430000
# 	@make test-sq y=2023 v=3440000
# 	@make test-sq y=2024 v=3450300
	@make test-sq y=2026 v=3530100
