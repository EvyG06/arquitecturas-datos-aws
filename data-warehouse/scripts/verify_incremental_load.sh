#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
[[ "$#" -eq 2 && "$1" == "--batch-id" && "$2" =~ ^[0-9]+$ ]] || {
  printf 'Usage: %s --batch-id UNUSED_NUMBER\n' "$0" >&2
  exit 2
}
[[ -f "$PROJECT_ROOT/warehouse/warehouse.duckdb" ]] || {
  printf 'Error: the warehouse is missing. Run scripts/run_pipeline.sh first.\n' >&2
  exit 1
}

batch_id="$2"
scripts_dir="$PROJECT_ROOT/scripts"

# Prints one line per fact (rows, distinct keys, total, content digest) and one per validated source.
warehouse_state() {
  compose exec -T airflow-scheduler python -c '
import hashlib
import duckdb

warehouse = duckdb.connect("/opt/warehouse/warehouse.duckdb", read_only=True)
facts = [
    ("fct_sales", "sale_sk", "subtotal"),
    ("fct_reviews", "review_sk", None),
    ("fct_navigation_events", "navigation_event_sk", None),
]
for table, key, amount in facts:
    amount_sql = f"coalesce(sum({amount}), 0)" if amount else "0"
    rows, unique_keys, total = warehouse.execute(
        f"select count(*), count(distinct {key}), {amount_sql} from marts.{table}"
    ).fetchone()
    content = warehouse.execute(f"select * from marts.{table} order by {key}").fetchall()
    print(table, rows, unique_keys, total, hashlib.sha256(repr(content).encode()).hexdigest()[:16])

sources = [
    ("valid_order_items", "select count(*), coalesce(sum(subtotal), 0) from intermediate.int_mysql__order_items__valid"),
    ("valid_reviews", "select count(*), 0 from intermediate.int_mongodb__reviews__valid"),
    ("valid_navigation_events", "select count(*), 0 from intermediate.int_mongodb__navigation_events__valid"),
]
for name, query in sources:
    rows, total = warehouse.execute(query).fetchone()
    print(name, rows, rows, total, "-")
'
}

# Fails when a fact does not match its validated source or contains duplicate keys.
assert_reconciled() {
  local state="$1" failures
  failures="$(awk '
    { rows[$1] = $2; unique[$1] = $3; total[$1] = $4 }
    END {
      pairs["fct_sales"] = "valid_order_items"
      pairs["fct_reviews"] = "valid_reviews"
      pairs["fct_navigation_events"] = "valid_navigation_events"
      for (fact in pairs) {
        source = pairs[fact]
        if (rows[fact] != rows[source] || unique[fact] != rows[fact] || total[fact] != total[source]) {
          print fact
        }
      }
    }' <<< "$state")"
  [[ -z "$failures" ]] || {
    printf 'Error: facts do not reconcile with their validated sources: %s\n%s\n' "$failures" "$state" >&2
    exit 1
  }
}

# Prints the value of one column for a named line of the warehouse state.
state_value() {
  awk -v name="$2" -v column="$3" '$1 == name { print $column }' <<< "$1"
}

printf 'Compiling the incremental models...\n'
dbt_command compile --select marts >/dev/null

baseline="$(warehouse_state)"
assert_reconciled "$baseline"
printf 'Baseline reconciles:\n%s\n' "$baseline"

first_append="$("$scripts_dir/append_batch.sh" --batch-id "$batch_id")"
printf '%s\n' "$first_append"
[[ "$first_append" == *"3 new orders"* ]] || {
  printf 'Error: batch %s is already present. Choose an unused batch ID.\n' "$batch_id" >&2
  exit 1
}
"$scripts_dir/run_pipeline.sh" --existing-sources

after_first="$(warehouse_state)"
assert_reconciled "$after_first"
for fact in fct_sales fct_reviews fct_navigation_events; do
  expected=$(( $(state_value "$baseline" "$fact" 2) + 3 ))
  [[ "$(state_value "$after_first" "$fact" 2)" -eq "$expected" ]] || {
    printf 'Error: %s did not grow by 3 rows after batch %s:\n%s\n' "$fact" "$batch_id" "$after_first" >&2
    exit 1
  }
done
printf 'After batch %s, every fact grew by 3 rows and reconciles:\n%s\n' "$batch_id" "$after_first"

second_append="$("$scripts_dir/append_batch.sh" --batch-id "$batch_id")"
printf '%s\n' "$second_append"
[[ "$second_append" == *"0 new orders"* ]] || {
  printf 'Error: repeating batch %s added orders.\n' "$batch_id" >&2
  exit 1
}
"$scripts_dir/run_pipeline.sh" --existing-sources

after_repeat="$(warehouse_state)"
[[ "$after_repeat" == "$after_first" ]] || {
  printf 'Error: repeating batch %s changed the facts:\n%s\n---\n%s\n' "$batch_id" "$after_first" "$after_repeat" >&2
  exit 1
}
printf 'Incremental load verified: repeating batch %s left every fact unchanged.\n' "$batch_id"
