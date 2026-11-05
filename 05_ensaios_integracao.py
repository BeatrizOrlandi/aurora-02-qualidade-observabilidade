# Databricks notebook source
# MAGIC %md
# MAGIC # Ensaios de qualidade e recuperação do Aurora
# MAGIC Fixtures pequenas e datasets isolados; nenhuma fonte externa. Falhas esperadas são verificadas, sem alterar a publicação principal.

# COMMAND ----------
import sys, json
from datetime import datetime, timezone
sys.path.insert(0, "/Workspace/Users/<SEU_USUARIO>/portfolio-estudos/01_lakehouse_aurora/runtime")
from aurora_runtime import *
from tracking_native import record
from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils.widgets.text("suite_id", "suite_v1")
suite = dbutils.widgets.get("suite_id")
assert suite.replace("_", "").isalnum()
dataset = "qa_" + suite
results = []

def check(name, expected, observed):
    ok = expected == observed
    results.append({"suite_id": suite, "test_id": name, "status": "pass" if ok else "fail",
                    "expected": json.dumps(expected, default=str), "observed": json.dumps(observed, default=str),
                    "created_at": datetime.now(timezone.utc)})
    if not ok:
        persist()
        raise AssertionError(f"{name}: esperado={expected}, observado={observed}")

def persist():
    if results:
        merge_frame(spark, spark.createDataFrame(results, schema=spark.table(TABLES["tests"]).schema), TABLES["tests"], ["suite_id", "test_id"])

def flow(batch, scenario, release, ds=dataset, backfill=False):
    config = Config(dataset=ds, batch=batch, scenario=scenario, release=suite + "_" + release, allow_backfill=backfill)
    generate_landing(config)
    ingest_bronze(spark, config)
    stats = transform(spark, config)
    gate = quality_gate(spark, config, raise_on_failure=False)
    print(json.dumps({"scenario": scenario, "release": config.release, "gate": gate}), flush=True)
    return config, stats, gate

# COMMAND ----------
baseline, stats, gate = flow("base", "base_small", "base")
check("baseline_acceptance", [20, 20, 0, 0], [stats["bronze_rows"], stats["accepted_rows"], stats["duplicate_rows"], stats["quarantine_rows"]])
check("baseline_gate", True, gate["passed"])
publish(spark, baseline)
totals = summary(spark, baseline)["totals"]
check("manual_golden_loans", 10, totals["loans"])
check("manual_golden_closed", 7, totals["closed_loans"])
check("manual_golden_open", 3, totals["open_loans"])
check("manual_golden_duration", 672.0, totals["total_duration_hours"])
initial_hash = gold_hash(spark, baseline)
first_reports = generate_reports(spark, baseline)
before_reports = spark.table(TABLES["reports"]).where(F.col("dataset_version") == dataset).count()
generate_reports(spark, baseline)
check("report_idempotence", before_reports, spark.table(TABLES["reports"]).where(F.col("dataset_version") == dataset).count())

# COMMAND ----------
repeated, stats, gate = flow("base", "base_small", "repeat")
check("bronze_reprocess_no_new_rows", 20, stats["bronze_rows"])
check("gold_reprocess_same_hash", initial_hash, gold_hash(spark, repeated))
publish(spark, repeated)
duplicates, stats, gate = flow("duplicates", "duplicates", "duplicates")
check("duplicate_reconciliation", [25, 20, 5, 0], [stats["bronze_rows"], stats["accepted_rows"], stats["duplicate_rows"], stats["quarantine_rows"]])
check("duplicate_warning_does_not_block", [True, 1], [gate["passed"], gate["warnings"]])
check("duplicate_gold_unchanged", initial_hash, gold_hash(spark, duplicates))
publish(spark, duplicates)

# COMMAND ----------
late, stats, gate = flow("late", "late_small", "late")
check("late_inside_window_passes", True, gate["passed"])
publish(spark, late)
totals = summary(spark, late)["totals"]
check("late_corrects_business_state", [10, 8, 2, 720.0], [totals["loans"], totals["closed_loans"], totals["open_loans"], totals["total_duration_hours"]])
protected = spark.table(TABLES["active"]).where(F.col("dataset_version") == dataset).first().release_id
invalid, stats, gate = flow("invalid", "invalid", "invalid")
check("invalid_rows_quarantined", 4, stats["quarantine_rows"])
check("critical_failure_blocks", False, gate["passed"])
try:
    publish(spark, invalid)
except ValueError:
    prevented = True
else:
    prevented = False
check("publish_refuses_failed_gate", True, prevented)
check("approved_release_preserved", protected, spark.table(TABLES["active"]).where(F.col("dataset_version") == dataset).first().release_id)
conflict, stats, gate = flow("conflict", "conflict", "conflict")
check("all_conflicting_source_rows_rejected", 3, stats["conflict_rows"])
check("two_distinct_payloads_in_conflict", 2, snapshot(spark, "quarantine", conflict).where(F.col("reason") == "event_key_conflict").select("raw_json").distinct().count())
check("conflicting_gate_blocked", False, gate["passed"])

# COMMAND ----------
backfill_dataset = dataset + "_backfill"
initial, stats, gate = flow("base", "base_small", "backfill_base", ds=backfill_dataset)
publish(spark, initial)
outside, stats, gate = flow("outside", "late_outside_small", "outside", ds=backfill_dataset)
check("late_outside_window_blocked", False, gate["passed"])
recovered, stats, gate = flow("outside", "late_outside_small", "backfill", ds=backfill_dataset, backfill=True)
check("explicit_backfill_recovers", True, gate["passed"])
publish(spark, recovered)
check("backfill_recomputes_closed", 8, summary(spark, recovered)["totals"]["closed_loans"])
persist()
output = {"suite_id": suite, "passed": len(results), "failed": sum(item["status"] != "pass" for item in results),
          "datasets": [dataset, backfill_dataset], "external_downloads": 0, "tests": [{k:v for k,v in item.items() if k != "created_at"} for item in results]}
output["tracking"] = record("/Users/<SEU_USUARIO>/portfolio-estudos/02_qualidade_observabilidade/experimentos/deteccao_falhas",
    "ensaios_" + suite, {"suite_id": suite, "fixture": "10_loans_7_closed_3_open", "runtime": "native_no_downloads"},
    {"passed": len(results), "failed": 0, "transformations": 9})
display(spark.table(TABLES["tests"]).where(F.col("suite_id") == suite))
dbutils.notebook.exit(json.dumps(output, default=str))
