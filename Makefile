.PHONY: all build check test test-verbose clippy fmt clean doc run-basic run-cluster help

# Default target
all: check clippy test

# Build the project
build:
	cargo build

# Build in release mode
release:
	cargo build --release

# Check code without building
check:
	cargo check

# Run all tests
test:
	cargo test

# Run tests with output
test-verbose:
	cargo test -- --nocapture

# Run a specific test
test-one:
	@test -n "$(TEST)" || (echo "Usage: make test-one TEST=<test_name>" && exit 1)
	cargo test $(TEST)

# Run clippy lints
clippy:
	cargo clippy

# Run clippy with warnings as errors
clippy-strict:
	cargo clippy -- -D warnings

# Format code
fmt:
	cargo fmt

# Check formatting without modifying
fmt-check:
	cargo fmt -- --check

# Generate documentation
doc:
	cargo doc --no-deps

# Open documentation in browser
doc-open:
	cargo doc --no-deps --open

# Clean build artifacts
clean:
	cargo clean

# Run basic example
run-basic:
	cargo run --example basic

# Run cluster node (usage: make run-cluster NODE=1)
run-cluster:
	@test -n "$(NODE)" || (echo "Usage: make run-cluster NODE=<node_id>" && exit 1)
	RUST_LOG=info cargo run --example cluster -- $(NODE)

# Run memberlist cluster example
run-memberlist:
	@test -n "$(NODE)" || (echo "Usage: make run-memberlist NODE=<node_id>" && exit 1)
	RUST_LOG=info cargo run --example memberlist-cluster -- $(NODE)

# Run auto-discovery example
run-discovery:
	cargo run --example auto-discovery

# Run all checks (CI)
ci: fmt-check clippy-strict test

# Watch for changes and run tests
watch:
	cargo watch -x test

# Show help
help:
	@echo "Crema - Distributed Cache Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Build targets:"
	@echo "  build          Build the project (debug)"
	@echo "  release        Build the project (release)"
	@echo "  check          Check code without building"
	@echo "  clean          Clean build artifacts"
	@echo ""
	@echo "Test targets:"
	@echo "  test           Run all tests"
	@echo "  test-verbose   Run tests with output"
	@echo "  test-one       Run a specific test (TEST=<name>)"
	@echo ""
	@echo "Code quality:"
	@echo "  clippy         Run clippy lints"
	@echo "  clippy-strict  Run clippy with warnings as errors"
	@echo "  fmt            Format code"
	@echo "  fmt-check      Check formatting"
	@echo "  ci             Run all CI checks"
	@echo ""
	@echo "Documentation:"
	@echo "  doc            Generate documentation"
	@echo "  doc-open       Generate and open documentation"
	@echo ""
	@echo "Examples:"
	@echo "  run-basic      Run basic example"
	@echo "  run-cluster    Run cluster node (NODE=1|2|3)"
	@echo "  run-memberlist Run memberlist cluster (NODE=1|2|3)"
	@echo "  run-discovery  Run auto-discovery example"
	@echo ""
	@echo "Other:"
	@echo "  watch          Watch for changes and run tests"
	@echo "  help           Show this help message"
