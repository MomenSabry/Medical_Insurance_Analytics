from datetime import datetime

BASE = "/Workspace/Users/mommensabry@gmail.com/Medical_Insurance_Analytics/lakehouse/DWH"

gold_scripts = [
    f"{BASE}/Init_Watermark.py",
    f"{BASE}/Gold_Dim_Date.py",
    f"{BASE}/Gold_Dim_Diagnosis.py",
    f"{BASE}/Gold_Dim_Procedure.py",
    f"{BASE}/Gold_Dim_Claim_Status.py",
    f"{BASE}/Gold_Dim_Patient.py",
    f"{BASE}/Gold_Dim_Hospital.py",
    f"{BASE}/Gold_Dim_Doctor.py",
    f"{BASE}/Gold_Dim_Department.py",
    f"{BASE}/Gold_Dim_Drug.py",
    f"{BASE}/Gold_Fact_Claims.py",
    f"{BASE}/Gold_Fact_Visits.py",
]

for script in gold_scripts:
    print(f"Running: {script}")
    exec(open(script).read())
    print(f"  ✓ Done")

print("All Gold scripts completed.")