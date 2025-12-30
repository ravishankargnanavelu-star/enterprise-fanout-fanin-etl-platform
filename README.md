# Enterprise Fan-Out / Fan-In ETL Orchestration on AWS

## Overview
This repository demonstrates an **enterprise-grade Fan-Out / Fan-In ETL orchestration architecture**
using **AWS Step Functions, AWS Lambda, and AWS Glue**.

A common orchestration validates raw data, enforces idempotency, routes execution by domain, runs
domain-specific ETL workflows in parallel, and finally performs post-processing to update metadata
and trigger downstream systems.

---

## Architecture Flow
```text
Event Trigger (S3)
   ↓
Common Orchestration
   ├── Validate Raw Data
   ├── Idempotency Check
   ├── Route by Domain
   └── Fan-Out Domain ETLs (Parallel)
           ├── Sales ETL
           ├── Payments ETL
           ├── Users ETL
           └── Logs ETL
                   ↓
             Post-Processing (Fan-In)
                   ↓
           Downstream Systems
