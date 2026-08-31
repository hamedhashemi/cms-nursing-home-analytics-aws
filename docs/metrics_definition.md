# Metrics Definition

## Core Metrics

### Total Direct HPRD
Total direct-care nursing hours per resident day.

**Formula:**  
(RN Hours + LPN Hours + CNA Hours) / Resident Census

### RN HPRD
Registered Nurse hours per resident day.

**Formula:**  
RN Hours / Resident Census

### CNA HPRD
Certified Nursing Assistant hours per resident day.

**Formula:**  
CNA Hours / Resident Census

### Contract Dependency %
Percentage of direct-care nursing hours provided by contract staff.

**Formula:**  
Contract Direct-Care Hours / Total Direct-Care Hours × 100

### Risk-Adjusted Rehospitalization Rate
CMS Measure 521: percentage of short-stay residents rehospitalized after nursing-home admission.

Lower values generally indicate better outcomes.

## Supporting Metrics

- LPN HPRD
- Short-Stay ED Rate
- Hospitalizations per 1,000 Long-Stay Resident Days
- Long-Stay ED Visits per 1,000 Resident Days
- Overall CMS Rating
- Staffing Rating
- Quality Measure Rating
- Health Inspection Rating

## Aggregation Rule

HPRD metrics are calculated using aggregate numerators and denominators:

`SUM(hours) / SUM(census)`

rather than averaging facility-day HPRD values.