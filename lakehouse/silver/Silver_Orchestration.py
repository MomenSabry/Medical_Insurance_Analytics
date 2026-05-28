# Orchestration script for running all Silver processing scripts
# Enhanced with error handling, logging, and run summary

import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SilverOrchestration")

# ── Script Registry ───────────────────────────────────────────────────────────
silver_scripts = [
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Bed",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Claim",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Claim_Approval",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Claim_Items",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Department",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Diagnosis",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Doctor",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Doctor_Schedule",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Drug",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Drug_Inventory",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Drug_Transaction",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Hospital",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Icu_Status",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Medical_Procedure",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Medical_Record",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Patient",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Patient_Feedback",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Prescription",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Referral",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Visit",
    "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/silver/Silver_Visit_Procedure",
]

# ── Audit Tracker ─────────────────────────────────────────────────────────────
audit_log = []

# ── Main Orchestration Loop ───────────────────────────────────────────────────
run_start = datetime.now()
logger.info(f"Starting Silver orchestration — {len(silver_scripts)} scripts")

for script in silver_scripts:
    start = datetime.now()
    try:
        logger.info(f"Running: {script}")
        dbutils.notebook.run(script, timeout_seconds=3600)
        duration = (datetime.now() - start).total_seconds()
        logger.info(f"  ✓ {script} completed in {duration:.1f}s")
        audit_log.append({"script": script, "status": "success", "duration_s": duration, "error": None})
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        logger.error(f"  ✗ {script} failed after {duration:.1f}s: {e}")
        audit_log.append({"script": script, "status": "failed", "duration_s": duration, "error": str(e)})

# ── Run Summary ───────────────────────────────────────────────────────────────
total_duration = (datetime.now() - run_start).total_seconds()
success = [r for r in audit_log if r["status"] == "success"]
failed  = [r for r in audit_log if r["status"] == "failed"]

logger.info("=" * 60)
logger.info(f"Orchestration complete in {total_duration:.1f}s")
logger.info(f"  ✓ Success : {len(success)} scripts")
logger.info(f"  ✗ Failed  : {len(failed)} scripts")
if failed:
    for r in failed:
        logger.error(f"    - {r['script']}: {r['error']}")
logger.info("=" * 60)

# ── Persist Audit Log ─────────────────────────────────────────────────────────
audit_df = spark.createDataFrame(audit_log)
(
    audit_df
    .withColumn("run_timestamp", F.lit(run_start.isoformat()))
    .write.format("delta").mode("append")
    .saveAsTable("medical_insurance.silver._orchestration_audit")
)
logger.info("Audit log written to medical_insurance.silver._orchestration_audit")