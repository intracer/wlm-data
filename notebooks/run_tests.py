# Databricks notebook source
# Runs the full pytest test suite on the Databricks cluster.
# Prerequisites: repo cloned via Databricks Repos, local CSV files present in data/

# COMMAND ----------
%pip install pytest

# COMMAND ----------
import sys
sys.path.insert(0, "src")

# COMMAND ----------
import pytest

# Run tests and print results. Exit code 0 = all passed.
retcode = pytest.main(["tests/", "-v", "--tb=short"])
assert retcode == 0, f"pytest exited with code {retcode} — see output above"
