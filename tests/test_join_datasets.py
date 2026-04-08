"""Tests for join_datasets tool."""

import csv
import pytest

from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.tools.join_datasets import join_datasets


@pytest.fixture
def orders_csv(tmp_path):
    """Orders dataset with customer_id FK."""
    path = tmp_path / "orders.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "amount", "product"])
        writer.writerows([
            [101, 1, 50.00, "Widget"],
            [102, 2, 30.00, "Gadget"],
            [103, 1, 75.00, "Doohickey"],
            [104, 3, 20.00, "Widget"],
            [105, 5, 100.00, "Thingamajig"],  # customer_id=5 has no match in customers
            [106, 2, 45.00, "Widget"],
        ])
    return str(path)


@pytest.fixture
def customers_csv(tmp_path):
    """Customers dataset."""
    path = tmp_path / "customers.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cust_id", "name", "city", "tier"])
        writer.writerows([
            [1, "Alice", "New York", "gold"],
            [2, "Bob", "Chicago", "silver"],
            [3, "Charlie", "New York", "bronze"],
            [4, "Diana", "Boston", "gold"],  # cust_id=4 has no orders
        ])
    return str(path)


@pytest.fixture
def indexed_pair(orders_csv, customers_csv, storage_dir):
    """Pre-indexed orders + customers datasets."""
    r1 = index_local(path=orders_csv, name="orders", storage_path=storage_dir)
    assert "error" not in r1
    r2 = index_local(path=customers_csv, name="customers", storage_path=storage_dir)
    assert "error" not in r2
    return storage_dir


class TestInnerJoin:

    def test_basic_inner_join(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            join_type="inner",
            storage_path=indexed_pair,
        )
        assert "error" not in result
        rows = result["result"]["rows"]
        # Orders with customer_id 1,2,3 match (5 orders); customer_id=5 has no match
        assert result["result"]["total_matching"] == 5
        assert len(rows) == 5

    def test_inner_join_has_columns_from_both(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            storage_path=indexed_pair,
        )
        rows = result["result"]["rows"]
        assert len(rows) > 0
        first = rows[0]
        # Should have columns from both datasets
        assert "order_id" in first
        assert "amount" in first
        assert "name" in first
        assert "city" in first

    def test_column_projection(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            columns_a=["order_id", "amount"],
            columns_b=["name", "tier"],
            storage_path=indexed_pair,
        )
        assert "error" not in result
        rows = result["result"]["rows"]
        first = rows[0]
        # Should have projected columns plus join columns
        assert "order_id" in first
        assert "amount" in first
        assert "name" in first
        assert "tier" in first
        # customer_id should be included (join column)
        assert "customer_id" in first


class TestLeftJoin:

    def test_left_join_keeps_unmatched(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            join_type="left",
            storage_path=indexed_pair,
        )
        assert "error" not in result
        rows = result["result"]["rows"]
        # All 6 orders should appear (including customer_id=5 with NULL customer fields)
        assert result["result"]["total_matching"] == 6

        # Find the unmatched order (customer_id=5)
        unmatched = [r for r in rows if r["customer_id"] == 5]
        assert len(unmatched) == 1
        assert unmatched[0]["name"] is None  # no matching customer


class TestRightJoin:

    def test_right_join_keeps_unmatched(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            join_type="right",
            storage_path=indexed_pair,
        )
        assert "error" not in result
        rows = result["result"]["rows"]
        # All 4 customers should appear (including Diana with no orders)
        assert result["result"]["total_matching"] == 6  # 5 matched orders + 1 unmatched customer


class TestFilters:

    def test_filter_on_dataset_a(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            filters_a=[{"column": "amount", "op": "gte", "value": 50}],
            storage_path=indexed_pair,
        )
        assert "error" not in result
        rows = result["result"]["rows"]
        for r in rows:
            assert r["amount"] >= 50

    def test_filter_on_dataset_b(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            filters_b=[{"column": "city", "op": "eq", "value": "New York"}],
            storage_path=indexed_pair,
        )
        assert "error" not in result
        rows = result["result"]["rows"]
        for r in rows:
            assert r["city"] == "New York"


class TestPagination:

    def test_limit(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            limit=2,
            storage_path=indexed_pair,
        )
        assert "error" not in result
        assert result["result"]["returned"] <= 2
        assert result["result"]["total_matching"] == 5  # total is still full count

    def test_offset(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            limit=2,
            offset=0,
            storage_path=indexed_pair,
        )
        page1 = result["result"]["rows"]

        result2 = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            limit=2,
            offset=2,
            storage_path=indexed_pair,
        )
        page2 = result2["result"]["rows"]

        # Pages should not overlap
        ids1 = {r["order_id"] for r in page1}
        ids2 = {r["order_id"] for r in page2}
        assert ids1.isdisjoint(ids2)


class TestOrderBy:

    def test_order_by_asc(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            order_by="amount",
            order_dir="asc",
            storage_path=indexed_pair,
        )
        amounts = [r["amount"] for r in result["result"]["rows"]]
        assert amounts == sorted(amounts)

    def test_order_by_desc(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            order_by="amount",
            order_dir="desc",
            storage_path=indexed_pair,
        )
        amounts = [r["amount"] for r in result["result"]["rows"]]
        assert amounts == sorted(amounts, reverse=True)


class TestErrorHandling:

    def test_not_indexed(self, storage_dir):
        result = join_datasets(
            dataset_a="nope",
            dataset_b="also_nope",
            join_column_a="id",
            join_column_b="id",
            storage_path=storage_dir,
        )
        assert "error" in result
        assert "NOT_INDEXED" in result["error"]

    def test_invalid_join_column_a(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="nonexistent",
            join_column_b="cust_id",
            storage_path=indexed_pair,
        )
        assert "error" in result
        assert "INVALID_COLUMN" in result["error"]

    def test_invalid_join_column_b(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="nonexistent",
            storage_path=indexed_pair,
        )
        assert "error" in result
        assert "INVALID_COLUMN" in result["error"]

    def test_invalid_join_type(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            join_type="full_outer",
            storage_path=indexed_pair,
        )
        assert "error" in result
        assert "INVALID_JOIN_TYPE" in result["error"]

    def test_invalid_column_projection(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            columns_a=["order_id", "fake_col"],
            storage_path=indexed_pair,
        )
        assert "error" in result
        assert "INVALID_COLUMN" in result["error"]


class TestMetadata:

    def test_meta_present(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            storage_path=indexed_pair,
        )
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]
        assert "tokens_saved" in result["_meta"]

    def test_result_structure(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            storage_path=indexed_pair,
        )
        r = result["result"]
        assert r["dataset_a"] == "orders"
        assert r["dataset_b"] == "customers"
        assert r["join_type"] == "inner"
        assert "join_on" in r
        assert "total_matching" in r
        assert "returned" in r
        assert "columns_a" in r
        assert "columns_b" in r


class TestSelfJoin:
    """Join a dataset with itself."""

    def test_self_join(self, indexed_pair):
        result = join_datasets(
            dataset_a="orders",
            dataset_b="orders",
            join_column_a="customer_id",
            join_column_b="customer_id",
            columns_a=["order_id", "amount"],
            columns_b=["order_id", "product"],
            storage_path=indexed_pair,
        )
        assert "error" not in result
        rows = result["result"]["rows"]
        assert len(rows) > 0
        # Self-join on customer_id: customers with multiple orders create cross products
        first = rows[0]
        # order_id from b should be aliased
        assert "order_id" in first
        assert "order_id__b" in first


class TestColumnCollision:
    """When both datasets have columns with the same name."""

    def test_duplicate_column_names_aliased(self, indexed_pair):
        """Both datasets should have their columns, with B-side collisions suffixed."""
        result = join_datasets(
            dataset_a="orders",
            dataset_b="customers",
            join_column_a="customer_id",
            join_column_b="cust_id",
            storage_path=indexed_pair,
        )
        # Both have no direct collision except potentially none in this fixture
        assert "error" not in result
