#!/usr/bin/env bash

echo "=========================================="
echo "  TEST EXECUTION - HEALTH COLOMBIA"
echo "=========================================="

case "$1" in
    "unit")
        echo "Running unit tests..."
        python -m pytest tests/ -v -m "unit" --tb=short
        ;;
    "integration")
        echo "Running integration tests..."
        python -m pytest tests/ -v -m "integration" --tb=short
        ;;
    "data")
        echo "Running data validation tests..."
        python -m pytest tests/ -v -m "data" --tb=short
        ;;
    "all")
        echo "Running ALL tests..."
        python -m pytest tests/ -v --tb=short
        ;;
    *)
        echo ""
        echo "Usage: ./run_tests.sh [option]"
        echo ""
        echo "Options:"
        echo "  unit         Run unit tests"
        echo "  integration  Run integration tests"
        echo "  data         Run data validation tests"
        echo "  all          Run all tests"
        echo ""
        echo "Example: ./run_tests.sh unit"
        ;;
esac

echo ""
echo "=========================================="
echo "  TESTS COMPLETED"
echo "=========================================="