# Engine-swap cost estimate

Study: "Keep your PySpark code, swap the engine." This note turns Phase 2 median wall times into a cost estimate for a platform architect choosing where a customer workload should run. It is an estimate, not an invoice and not a cloud benchmark.

- Region priced: Azure West Europe (`armRegionName=westeurope`, location EU West).
- Currency: USD, the currency Microsoft uses for Azure retail prices.
- Prices accessed: 2026-11-26 (Europe/Istanbul).
- Wall times: `results/final/phase2_totals.csv`, median of two complete published repetitions (rep 3 dropped). TPC-H SF30 and the study ETL set. Conditions included: local disk, MinIO loopback, and toxiproxy +10 ms per request (`proxy10`). The `proxy0` control is in the totals file but is not costed here.
- Companion spreadsheet: `article/cost_table.csv`.

The measured box is a single node with 8 cores and about 16 GB RAM, with a 9 GiB cgroup memory cap. That is not the same as a cloud VM or a Fabric or Databricks cluster. Applying our wall time to a cloud rate assumes the job would finish in the same number of seconds there. I/O path, CPU generation, noisy neighbors, and service overhead will differ. Treat the dollar figures as a planning range, then re-measure on the target platform before a customer commitment.

## Verified rates used in the totals

All dollar rates below were read from the official Azure Retail Prices API on 2026-11-26, or (for the Databricks DBU count only) from the official Azure Databricks pricing page, which publishes DBU counts even when it hides dollar amounts as `$-`.

| Input | Value | Where it was verified |
|---|---|---|
| Azure Linux VM `Standard_F8s_v2` (8 vCPU, 16 GiB), pay as you go | $0.388 per hour | Azure Retail Prices API, product `Virtual Machines FSv2 Series`, sku `F8s v2`, meter `F8s v2`, meterId `86563e2c-da39-440f-8e8c-a1c94711b118`, effectiveStartDate 2025-10-01. Linux is the series product without "Windows" in the name (the Windows meter for the same size is $0.74 and is not used). |
| Azure Linux VM `Standard_D8ds_v5` (8 vCPU, 32 GiB), pay as you go | $0.544 per hour | Azure Retail Prices API, product `Virtual Machines Ddsv5 Series`, sku `Standard_D8ds_v5`, meter `D8ds v5`, meterId `2a477454-b363-5169-bcd6-bacac0dbabc1`, effectiveStartDate 2021-11-01. |
| Azure Databricks Premium Jobs Compute | $0.30 per DBU-hour | Azure Retail Prices API, product `Azure Databricks`, sku `Premium Jobs Compute`, meter `Premium Jobs Compute DBU`, meterId `8ee07b23-82ae-430e-a417-a050463ff4ee`, region West Europe, effectiveStartDate 2019-03-01. |
| DBU count for `D8ds v5` | 2.00 DBU per hour | Official Azure Databricks pricing page, Ddsv5 series table: instance `D8ds v5`, 8 vCPU, 32.00 GiB, DBU Count 2.00. |
| Combined Databricks Jobs hourly rate used in totals | $1.144 per hour | $0.544 VM + 2.00 x $0.30 DBU. Single-node cluster (one `Standard_D8ds_v5`, no worker nodes, no Photon). |
| Microsoft Fabric pay-as-you-go CU rate, including the autoscale-for-Spark meter | $0.22 per CU-hour | Azure Retail Prices API, product `Fabric Capacity`, sku `autoscale for Spark Capacity Usage`, meter `autoscale for Spark Capacity Usage CU`, meterId `dcc3f574-0e57-5d34-ab6c-1e70659f7da7`, effectiveStartDate 2025-07-01. The same $0.22 per CU-hour is the modal West Europe rate for Fabric `Capacity Usage CU` consumption meters. The official Fabric pricing page states that Autoscale Billing for Spark uses standard regional pay-as-you-go CU-hour rates, and that reserved-capacity discounts do not apply to that meter. |
| Fabric Spark mapping | 1 CU = 2 Spark vCores, so 0.5 CU-hour per Spark vCore-hour | Official Learn docs (billing and on-demand/autoscale overviews). |
| Combined Fabric Spark hourly rate used in totals | $0.88 per hour | 8 Spark vCores x 0.5 CU-hour per vCore-hour x $0.22. |

The Azure Databricks Standard tier was retired on 1 October 2026. The Standard Jobs Compute meter is still in the price feed at $0.15 per DBU-hour; it is not used in any total here. Premium ($0.30) is the rate that applies.

## How each dollar figure is computed

```
cost_per_run_usd    = median_wall_seconds / 3600 x hourly_rate_usd
cost_per_month_usd  = cost_per_run_usd x 24 runs/day x 30 days
                    = cost_per_run_usd x 720
```

Per-run amounts are rounded to four decimal dollars, monthly amounts to two decimal dollars, using half-up rounding. The CSV also keeps unrounded values. Databricks and Fabric both bill per second for these meters (Databricks pricing FAQ; Fabric capacity is billed through Azure). This estimate uses the measured wall time as the billed duration.

## Assumptions (all of them)

1. The measured single-node box is not equal to the cloud. Times come from our lab (Spark `local[8]`, 8 cores, about 16 GB RAM, 9 GiB cgroup cap, MinIO or local disk). They are applied unchanged to cloud hourly rates.
2. Cluster and VM startup are excluded. We did not measure Databricks cluster start, VM boot, or image pull. Classic Databricks Jobs Compute bills VM + DBU while the cluster is up, so a real Jobs Compute run will cost more than the figures below by whatever start and shutdown time the workspace actually burns. Official Fabric Spark pool docs say customers are not charged for cluster acquisition or Spark context initialization, and that on-demand/autoscale billing applies only to active job compute with no idle compute cost. Fabric estimates therefore follow that documented rule. I still exclude any unmeasured session leftover (Fabric's default session expiration is 20 minutes if a session is left open).
3. The Databricks comparison is a single-node Premium Jobs Compute cluster of `Standard_D8ds_v5` (8 vCPU, 32 GiB, 2.00 DBU/hour). That matches our 8 cores but has twice the RAM of the box. A driver-plus-worker Jobs cluster would cost a multiple of this rate and is not estimated. Photon is off (the official page notes that enabling Photon increases the DBU count; we have no official multiplier in this file, so Photon is omitted).
4. The Fabric comparison is on-demand / autoscale Spark billing for 8 Spark vCores, which is the core count of an F4 (4 CU) capacity. This is pay-for-active-compute, not an always-on F-SKU bill. Fabric Spark node RAM is not taken from an official page in this file and is left unverified.
5. The single-VM comparison is one Linux `Standard_F8s_v2` (8 vCPU, 16 GiB) started for the job and stopped when it finishes, billed only for measured wall time. OS disk, public IP, bandwidth, and storage are excluded. Sail's local TPC-H peak RSS was 9.276 GB, so 16 GiB is tight; a verified alternative is `Standard_D8s_v5` (8 vCPU, 32 GiB) at $0.46 per hour in the same region and API pull. That alternative is not used in the totals.
6. DuckDB, Sail, and Comet are costed only on the single Azure VM. They are not official Databricks Jobs Compute or Fabric Spark SKUs in this study, so no DBU or CU total is given for them.
7. Spark 4.1.3 and 4.2.0 wall times (classic and analytics streams) are the only times applied to Databricks and Fabric. Comet wall times are not applied to those platforms (we ran Comet on open-source Spark in the lab; a Databricks or Fabric Comet deployment was not measured and is not priced).
8. Local-disk wall times are costed only on the Azure VM. They are not applied to Databricks or Fabric, because those local times are not an object-store cloud runtime.
9. 24 runs per day, 30-day month, 720 runs, no overlaps, no failed retries, no idle between queries beyond what is already inside the measured total. Failed F4 attempts and stalled jobs from the study are not added as extra billed hours.
10. Pay-as-you-go list prices only. No reservations, savings plans, committed-use discounts, spot / low-priority VMs, or negotiated rates.
11. Storage (S3, ADLS, OneLake, disks), networking, NAT, Private Link, public IPs, monitoring, and support plans are excluded.
12. Results of the 22 TPC-H queries were identical across engines in this matrix. The cost table is about time and money, not about whether a swap is safe. Use `article/migration_checklist.md` for that.

## Always-on F-SKU capacity (not used in engine totals)

If a customer instead buys Microsoft Fabric capacity and leaves it running 24 hours a day for 30 days, the compute bill is the CU count times $0.22 per CU-hour times 720 hours, regardless of how long this one job takes, and the capacity is shared with every other Fabric workload on that SKU:

| SKU | CU | Always-on PAYG for 720 hours (West Europe, USD) |
|---|---|---|
| F2 | 2 | $316.80 |
| F4 | 4 | $633.60 |
| F8 | 8 | $1,267.20 |

F4 is the SKU whose CU count matches the 8 Spark vCores used in the per-run Fabric estimate. These always-on figures are a purchasing-model reference only. They are not added into the engine comparison totals, because per-run cost then depends on how the capacity is shared and whether it is paused, which we did not measure.

## Estimates

"Not applicable" means that engine was not priced on that platform, for the reasons in the assumptions. Azure VM figures exist for every engine so that open-source Spark, Comet, Sail, and DuckDB can be compared on the same verified VM rate.

### Loopback MinIO, TPC-H SF30

| config | median wall s | n | peak RSS GB | Azure VM $/run | Azure VM $/month | Databricks Jobs $/run | Databricks Jobs $/month | Fabric Spark $/run | Fabric Spark $/month |
|---|---|---|---|---|---|---|---|---|---|
| comet420-analytics | 289.055 | 2 | 4.867 | 0.0312 | 22.43 | not applicable | not applicable | not applicable | not applicable |
| comet420-classic | 281.6 | 2 | 5.321 | 0.0304 | 21.85 | not applicable | not applicable | not applicable | not applicable |
| duckdb | 55.33 | 2 | 5.521 | 0.0060 | 4.29 | not applicable | not applicable | not applicable | not applicable |
| sail-noreorder | 81.079 | 2 | 8.48 | 0.0087 | 6.29 | not applicable | not applicable | not applicable | not applicable |
| sail-reorder | 71.98 | 2 | 8.693 | 0.0078 | 5.59 | not applicable | not applicable | not applicable | not applicable |
| spark413-analytics | 510.628 | 2 | 7.64 | 0.0550 | 39.62 | 0.1623 | 116.83 | 0.1248 | 89.87 |
| spark413-classic | 434.267 | 2 | 7.553 | 0.0468 | 33.70 | 0.1380 | 99.36 | 0.1062 | 76.43 |
| spark420-analytics | 443.983 | 2 | 7.601 | 0.0479 | 34.45 | 0.1411 | 101.58 | 0.1085 | 78.14 |
| spark420-classic | 440.087 | 2 | 7.542 | 0.0474 | 34.15 | 0.1398 | 100.69 | 0.1076 | 77.46 |

### Proxy +10 ms per request, TPC-H SF30

| config | median wall s | n | peak RSS GB | Azure VM $/run | Azure VM $/month | Databricks Jobs $/run | Databricks Jobs $/month | Fabric Spark $/run | Fabric Spark $/month |
|---|---|---|---|---|---|---|---|---|---|
| comet420-analytics | 292.885 | 2 | 4.918 | 0.0316 | 22.73 | not applicable | not applicable | not applicable | not applicable |
| comet420-classic | 308.812 | 2 | 5.387 | 0.0333 | 23.96 | not applicable | not applicable | not applicable | not applicable |
| duckdb | 98.582 | 2 | 5.5 | 0.0106 | 7.65 | not applicable | not applicable | not applicable | not applicable |
| sail-noreorder | 122.165 | 2 | 8.298 | 0.0132 | 9.48 | not applicable | not applicable | not applicable | not applicable |
| sail-reorder | 121.165 | 2 | 8.362 | 0.0131 | 9.40 | not applicable | not applicable | not applicable | not applicable |
| spark413-analytics | 621.88 | 2 | 7.643 | 0.0670 | 48.26 | 0.1976 | 142.29 | 0.1520 | 109.45 |
| spark413-classic | 691.361 | 2 | 7.506 | 0.0745 | 53.65 | 0.2197 | 158.18 | 0.1690 | 121.68 |
| spark420-analytics | 532.065 | 2 | 7.577 | 0.0573 | 41.29 | 0.1691 | 121.74 | 0.1301 | 93.64 |
| spark420-classic | 702.024 | 2 | 7.532 | 0.0757 | 54.48 | 0.2231 | 160.62 | 0.1716 | 123.56 |

### Loopback MinIO, ETL

| config | median wall s | n | peak RSS GB | Azure VM $/run | Azure VM $/month | Databricks Jobs $/run | Databricks Jobs $/month | Fabric Spark $/run | Fabric Spark $/month |
|---|---|---|---|---|---|---|---|---|---|
| comet420-analytics | 71.498 | 2 | 4.905 | 0.0077 | 5.55 | not applicable | not applicable | not applicable | not applicable |
| comet420-classic | 73.024 | 2 | 5.037 | 0.0079 | 5.67 | not applicable | not applicable | not applicable | not applicable |
| duckdb | 23.149 | 2 | 7.282 | 0.0025 | 1.80 | not applicable | not applicable | not applicable | not applicable |
| sail-noreorder | 19.596 | 2 | 4.126 | 0.0021 | 1.52 | not applicable | not applicable | not applicable | not applicable |
| sail-reorder | 20.05 | 2 | 4.2 | 0.0022 | 1.56 | not applicable | not applicable | not applicable | not applicable |
| spark413-analytics | 131.671 | 2 | 7.526 | 0.0142 | 10.22 | 0.0418 | 30.13 | 0.0322 | 23.17 |
| spark413-classic | 119.225 | 2 | 7.463 | 0.0128 | 9.25 | 0.0379 | 27.28 | 0.0291 | 20.98 |
| spark420-analytics | 112.642 | 2 | 7.415 | 0.0121 | 8.74 | 0.0358 | 25.77 | 0.0275 | 19.82 |
| spark420-classic | 115.546 | 2 | 7.421 | 0.0125 | 8.97 | 0.0367 | 26.44 | 0.0282 | 20.34 |

### Proxy +10 ms per request, ETL

| config | median wall s | n | peak RSS GB | Azure VM $/run | Azure VM $/month | Databricks Jobs $/run | Databricks Jobs $/month | Fabric Spark $/run | Fabric Spark $/month |
|---|---|---|---|---|---|---|---|---|---|
| comet420-analytics | 150.869 | 2 | 5.013 | 0.0163 | 11.71 | not applicable | not applicable | not applicable | not applicable |
| comet420-classic | 143.161 | 2 | 5.094 | 0.0154 | 11.11 | not applicable | not applicable | not applicable | not applicable |
| duckdb | 23.584 | 2 | 7.295 | 0.0025 | 1.83 | not applicable | not applicable | not applicable | not applicable |
| sail-noreorder | 26.241 | 2 | 4.157 | 0.0028 | 2.04 | not applicable | not applicable | not applicable | not applicable |
| sail-reorder | 36.944 | 2 | 4.146 | 0.0040 | 2.87 | not applicable | not applicable | not applicable | not applicable |
| spark413-analytics | 219.089 | 2 | 7.488 | 0.0236 | 17.00 | 0.0696 | 50.13 | 0.0536 | 38.56 |
| spark413-classic | 221.676 | 2 | 7.44 | 0.0239 | 17.20 | 0.0704 | 50.72 | 0.0542 | 39.01 |
| spark420-analytics | 202.79 | 2 | 7.468 | 0.0219 | 15.74 | 0.0644 | 46.40 | 0.0496 | 35.69 |
| spark420-classic | 229.351 | 2 | 7.509 | 0.0247 | 17.80 | 0.0729 | 52.48 | 0.0561 | 40.37 |

### Local disk, TPC-H SF30 (Azure VM only)

| config | median wall s | n | peak RSS GB | Azure VM $/run | Azure VM $/month | Databricks Jobs $/run | Databricks Jobs $/month | Fabric Spark $/run | Fabric Spark $/month |
|---|---|---|---|---|---|---|---|---|---|
| comet420-default | 237.253 | 2 | 4.884 | 0.0256 | 18.41 | not applicable | not applicable | not applicable | not applicable |
| duckdb | 48.412 | 2 | 5.436 | 0.0052 | 3.76 | not applicable | not applicable | not applicable | not applicable |
| sail-noreorder | 49.584 | 2 | 9.276 | 0.0053 | 3.85 | not applicable | not applicable | not applicable | not applicable |
| sail-reorder | 52.796 | 2 | 9.185 | 0.0057 | 4.10 | not applicable | not applicable | not applicable | not applicable |
| spark413-default | 383.934 | 2 | 7.381 | 0.0414 | 29.79 | not applicable | not applicable | not applicable | not applicable |
| spark420-default | 386.568 | 2 | 7.41 | 0.0417 | 30.00 | not applicable | not applicable | not applicable | not applicable |


## How to read the headline numbers

On loopback MinIO TPC-H SF30, 24 runs a day for 30 days:

- DuckDB on a single `Standard_F8s_v2` is $0.0060 per run, $4.29 per month (55.33 s).
- Sail with join reorder on is $0.0078 per run, $5.59 per month (71.98 s). Sail with reorder off is $6.29 per month (81.079 s).
- Comet 1.2.0 classic is $0.0304 per run, $21.85 per month (281.6 s).
- Open-source Spark 4.2.0 classic on the same VM is $0.0474 per run, $34.15 per month (440.087 s), about 8 times the DuckDB VM bill, because it took about 8 times as long.
- The same Spark 4.2.0 classic wall time, billed as Fabric on-demand Spark (8 vCores), is $0.1076 per run, $77.46 per month, about 18 times the DuckDB VM bill.
- The same wall time, billed as Databricks Premium Jobs Compute on one `Standard_D8ds_v5`, is $0.1398 per run, $100.69 per month, about 23.5 times the DuckDB VM bill. That Databricks figure still excludes cluster startup, so a real Jobs Compute job will be higher.

On the +10 ms path, AAL (the Spark S3A analytics stream) matters for Spark 4.2.0 TPC-H: classic is 702.024 s, analytics is 532.065 s. That difference is $38.88 per month on the Databricks rate ($160.62 vs $121.74) and $29.92 per month on the Fabric rate ($123.56 vs $93.64), still excluding startup. Spark 4.1.3 analytics is not a win on loopback (510.628 s vs 434.267 s classic), and that slower loopback time would raise a Databricks Jobs bill from $99.36 to $116.83 per month. Comet GET counts did not change with AAL, and Comet is not priced on Databricks or Fabric here.

ETL is cheaper than TPC-H in every row because it is shorter. Loopback ETL on Sail (no reorder) is $1.52 per month on the VM; Spark 4.2.0 analytics on Databricks is $25.77 per month for the same 720-run calendar, still excluding startup.

None of these totals include a correctness tax. If a swap silently changes timezone or decimal results, the platform bill is the smaller problem. See the migration checklist.

## What was left out because it was not verified

- Databricks or Fabric cluster startup and shutdown time (excluded, as stated).
- Photon DBU multiplier (officially exists, official factor not recorded in this file).
- Fabric always-on overage (the API lists Capacity Overage at $0.66 per CU-hour in West Europe, three times the $0.22 PAYG rate). Not used in totals.
- A one-minute minimum on Fabric billing, if any. Not applied, because it was not verified on the official pages opened for this file.
- AWS or GCP Databricks list prices. This file prices Azure West Europe only.
- Reserved, savings-plan, committed-use, or spot rates, even though the API returns some of them.
- Storage, network, disk, and IP charges.

## Official sources

Prices and rules were taken from these official pages and APIs on 2026-11-26. Third-party price aggregators were not used as a source.

1. Azure Retail Prices API documentation: https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices
2. Azure Retail Prices API (West Europe, USD), Microsoft Fabric consumption: `https://prices.azure.com/api/retail/prices?currencyCode=USD&$filter=serviceName eq 'Microsoft Fabric' and armRegionName eq 'westeurope' and priceType eq 'Consumption'`
3. Azure Retail Prices API (West Europe, USD), Azure Databricks: `https://prices.azure.com/api/retail/prices?currencyCode=USD&$filter=serviceName eq 'Azure Databricks' and armRegionName eq 'westeurope'`
4. Azure Retail Prices API (West Europe, USD), `Standard_F8s_v2`: `https://prices.azure.com/api/retail/prices?currencyCode=USD&$filter=armSkuName eq 'Standard_F8s_v2' and armRegionName eq 'westeurope' and type eq 'Consumption'`
5. Azure Retail Prices API (West Europe, USD), `Standard_D8ds_v5`: `https://prices.azure.com/api/retail/prices?currencyCode=USD&$filter=armSkuName eq 'Standard_D8ds_v5' and armRegionName eq 'westeurope' and type eq 'Consumption'`
6. Azure Retail Prices API (West Europe, USD), `Standard_D8s_v5` (alternative, not in totals): `https://prices.azure.com/api/retail/prices?currencyCode=USD&$filter=armSkuName eq 'Standard_D8s_v5' and armRegionName eq 'westeurope' and type eq 'Consumption'`
7. Azure Databricks pricing (DBU counts by VM size; Standard tier retirement 1 October 2026; Jobs Compute plus VM billing; Photon increases DBU count): https://azure.microsoft.com/en-us/pricing/details/databricks/
8. Microsoft Fabric pricing (F-SKU table; Autoscale Billing for Spark uses standard regional PAYG CU-hour rates; reserved discounts do not apply): https://azure.microsoft.com/en-us/pricing/details/microsoft-fabric/
9. Apache Spark billing in Fabric (1 CU maps to two Spark vCores; autoscale bills only active job compute; starter and custom pools are not billed for cluster acquisition or context initialization; default session expiration 20 minutes): https://learn.microsoft.com/en-us/fabric/data-engineering/billing-capacity-management-for-spark
10. On-demand / autoscale billing for Spark in Fabric (usage rate 0.5 CU hour, active job compute only): https://learn.microsoft.com/en-us/fabric/data-engineering/autoscale-billing-for-spark-overview
11. Databricks pricing FAQ (pay as you go, per-second granularity): https://www.databricks.com/product/pricing
12. Measured wall times: `results/final/phase2_totals.csv` and `results/final/TABLES.md`.
