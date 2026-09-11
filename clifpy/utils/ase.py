#!/usr/bin/env python3
"""
ASE (Adult Sepsis Event) Production Implementation
CDC Sepsis Surveillance Calculator - Standalone Version

This script implements the CDC Adult Sepsis Event (ASE) surveillance definition
as specified in the March 2018 CDC Sepsis Surveillance Toolkit.
CDC ASE Definition:
**Sepsis = Component A (Presumed Serious Infection) + Component B (Acute Organ Dysfunction)**

- **Component A**: Blood culture + ≥4 Qualifying Antimicrobial Days (QAD)
- **Component B**: ≥1 organ dysfunction criterion within ±2 calendar days of blood culture

Component A: Presumed Serious Infection

### CDC Definition (Pages 5-8):
1. Blood culture obtained (result not required)
2. ≥4 Qualifying Antimicrobial Days (QAD) starting within ±2 calendar days of blood culture
   - New antimicrobial (not given in prior 2 days)
   - At least one IV/IM antimicrobial required
   - 1-day gaps allowed between antimicrobial days

For Identifying QAD, CDC Requirements:
- "New antimicrobial" = not given in prior 2 calendar days
- First antimicrobial must be IV/IM and within ±2 calendar days of blood culture
- Need ≥4 consecutive days (allowing 1-day gaps)
- CDC allows QAD < 4 days if patient dies, transfers to acute care, or goes to hospice (Page 8)

Component B: Acute Organ Dysfunction

### CDC Definition (Page 5 and 9):
At least ONE organ dysfunction criterion within ±2 calendar days of blood culture:
1. **Renal**: Creatinine ≥2x baseline (exclude ESRD)
2. **Hepatic**: Bilirubin ≥2.0 mg/dL AND ≥2x baseline
3. **Coagulation**: Platelets <100 cells/μL AND ≥50% decline from baseline ≥100
4. **Metabolic**: Lactate ≥2.0 mmol/L
5. **Cardiovascular**: New vasopressor initiation
6. **Respiratory**: New mechanical ventilation initiation

POST-Processing Apply RIT (Repeat Infection Timeframe) Post-Processing

CDC Page 9: For hospital-onset events, apply 14-day repeat infection timeframe to avoid duplicate counting

Memory-optimized implementation with staged data loading and immediate cleanup.
"""

# ==============================================================================
# 1. IMPORTS AND SETUP
# ==============================================================================
import pandas as pd
import numpy as np
import duckdb
import json
import gc
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Union

# CLIF imports - lazy loaded inside functions to avoid circular imports

# ==============================================================================
# 2. CONSTANTS AND CONFIGURATION
# ==============================================================================

# CDC ASE Window and Timeframe Constants
WINDOW_DAYS = 2  # ±2 calendar days around blood culture
RIT_DAYS = 14    # Repeat infection timeframe
BILI_MULTIPLIER = 2.0  # 100% increase from baseline (per CDC toolkit)

# Outlier thresholds for lab values
OUTLIERS = {
    "creatinine_max": 20,
    "bilirubin_max": 80,
    "platelet_max": 2000,
    "lactate_max": 30,
}

# ESRD ICD-10 codes (for exclusion from renal dysfunction)
ESRD_CODES = [
    "n186",    # End stage renal disease
    "z4931",   # Encounter for continuous renal replacement therapy (CRRT) for ESRD (CMS/HCC)
    "z4901",   # Encounter regarding vascular access for dialysis for end-stage renal disease (CMS/HCC)
    "i120",    # Hypertensive chronic kidney disease with stage 5 CKD or ESRD
    "i1311",   # Hypertensive heart and chronic kidney disease with heart failure and stage 5 CKD
    "i132",    # Hypertensive heart and chronic kidney disease with ESRD (alternate code)
    "i120",    # Hypertensive chronic kidney disease with stage 5 CKD or ESRD (alternate code)
    "i272",    # Pulmonary hypertension associated with ESRD on dialysis (CMS/HCC)
]

# Configuration flags
APPLY_RIT = True
RIT_ONLY_HOSPITAL_ONSET = True

# ==============================================================================
# 3. SQL QUERY DEFINITIONS 
# ==============================================================================

# QAD Calculation Query 
QAD_QUERY = """
/* 0) Cultures */
WITH cultures AS (
  SELECT
    hospitalization_id,
    bc_id,
    culture_time,
    DATE(culture_day) AS culture_day
  FROM blood_cultures
  WHERE culture_time IS NOT NULL
),

/* 1) Antibiotics at day level (vancomycin exception) */
abx_day AS (
  SELECT DISTINCT
    a.hospitalization_id,
    DATE(a.med_admin_day) AS antibiotic_day,
    CASE
      WHEN LOWER(a.med_category) = 'vancomycin' AND a.is_iv_im = 1 THEN 'vancomycin_iv'
      WHEN LOWER(a.med_category) = 'vancomycin' AND a.is_iv_im = 0 THEN 'vancomycin_oral'
      ELSE a.med_category
    END AS med_category_tracked,
    a.is_iv_im
  FROM antibiotics a
  JOIN hospitalizations h
    ON a.hospitalization_id = h.hospitalization_id
  WHERE a.med_admin_day IS NOT NULL
    AND a.med_admin_day >= h.admission_dttm
    AND a.med_admin_day <= h.discharge_dttm
),

/* 2) Mark new courses per drug (new if gap > 2 days) */
abx_course_marked AS (
  SELECT
    hospitalization_id,
    med_category_tracked,
    antibiotic_day,
    CASE
      WHEN LAG(antibiotic_day) OVER (
        PARTITION BY hospitalization_id, med_category_tracked
        ORDER BY antibiotic_day
      ) IS NULL THEN 1
      WHEN antibiotic_day - LAG(antibiotic_day) OVER (
        PARTITION BY hospitalization_id, med_category_tracked
        ORDER BY antibiotic_day
      ) > 2 THEN 1
      ELSE 0
    END AS new_course_flag,
    MAX(is_iv_im) OVER (
      PARTITION BY hospitalization_id, med_category_tracked, antibiotic_day
    ) AS any_iv_im_that_day
  FROM abx_day
),

/* 2b) Assign course_id */
abx_courses AS (
  SELECT
    hospitalization_id,
    med_category_tracked,
    SUM(new_course_flag) OVER (
      PARTITION BY hospitalization_id, med_category_tracked
      ORDER BY antibiotic_day
      ROWS UNBOUNDED PRECEDING
    ) AS course_id,
    antibiotic_day,
    any_iv_im_that_day
  FROM abx_course_marked
),

/* 3a) Course bounds */
course_bounds AS (
  SELECT
    hospitalization_id,
    med_category_tracked,
    course_id,
    MIN(antibiotic_day) AS course_start_day,
    MAX(antibiotic_day) AS course_end_day
  FROM abx_courses
  GROUP BY hospitalization_id, med_category_tracked, course_id
),

/* 3b) Whether course START DAY is IV/IM */
course_intervals AS (
  SELECT
    b.hospitalization_id,
    b.med_category_tracked,
    b.course_id,
    b.course_start_day,
    b.course_end_day,
    MAX(
      CASE
        WHEN a.antibiotic_day = b.course_start_day THEN a.any_iv_im_that_day
        ELSE 0
      END
    ) AS start_day_is_iv_im
  FROM course_bounds b
  JOIN abx_courses a
    ON a.hospitalization_id = b.hospitalization_id
   AND a.med_category_tracked = b.med_category_tracked
   AND a.course_id = b.course_id
  GROUP BY
    b.hospitalization_id, b.med_category_tracked, b.course_id,
    b.course_start_day, b.course_end_day
),

/* 4) Join cultures to courses; mark starts in the ±2 day window */
culture_course_window AS (
  SELECT
    c.hospitalization_id,
    c.bc_id,
    c.culture_time,
    c.culture_day,
    ci.med_category_tracked,
    ci.course_start_day,
    ci.course_end_day,
    ci.start_day_is_iv_im,
    CASE
      WHEN ci.course_start_day BETWEEN c.culture_day - 2 AND c.culture_day + 2 THEN 1
      ELSE 0
    END AS course_start_in_window
  FROM cultures c
  JOIN course_intervals ci
    ON c.hospitalization_id = ci.hospitalization_id
),

/* 4b) Anchor: earliest new antimicrobial start in window (any route),
       and require at least one new parenteral start in window */
qad_anchor AS (
  SELECT
    hospitalization_id,
    bc_id,
    culture_time,
    culture_day,
    MIN(CASE WHEN course_start_in_window = 1 THEN course_start_day END) AS qad_start_day,
    MAX(CASE
          WHEN course_start_in_window = 1 AND start_day_is_iv_im = 1 THEN 1
          ELSE 0
        END) AS has_new_parenteral_in_window
  FROM culture_course_window
  GROUP BY hospitalization_id, bc_id, culture_time, culture_day
  HAVING MIN(CASE WHEN course_start_in_window = 1 THEN course_start_day END) IS NOT NULL
),

/* 5) Eligible courses: only those starting on/after qad_start_day */
eligible_courses AS (
  SELECT DISTINCT
    a.hospitalization_id,
    a.bc_id,
    a.culture_time,
    a.culture_day,
    a.qad_start_day,
    a.has_new_parenteral_in_window,
    w.med_category_tracked,
    w.course_start_day,
    w.course_end_day
  FROM qad_anchor a
  JOIN culture_course_window w
    ON a.hospitalization_id = w.hospitalization_id
   AND a.bc_id = w.bc_id
  WHERE w.course_start_day >= a.qad_start_day
),

/* QC: meds started in window (anchors) */
qc_anchor_meds AS (
  SELECT
    hospitalization_id,
    bc_id,
    string_agg(DISTINCT med_category_tracked, ', ') AS anchor_meds_in_window,
    string_agg(
      DISTINCT CASE WHEN start_day_is_iv_im = 1 THEN med_category_tracked ELSE NULL END,
      ', '
    ) AS anchor_parenteral_meds_in_window
  FROM culture_course_window
  WHERE course_start_in_window = 1
  GROUP BY hospitalization_id, bc_id
),

/* QC: meds eligible to contribute after QAD starts */
qc_run_meds AS (
  SELECT
    hospitalization_id,
    bc_id,
    string_agg(DISTINCT med_category_tracked, ', ') AS run_meds
  FROM eligible_courses
  GROUP BY hospitalization_id, bc_id
),

/* 6) Expand covered days for eligible courses (counts single-gap q48h days) */
covered_days AS (
  SELECT DISTINCT
    hospitalization_id,
    bc_id,
    culture_time,
    culture_day,
    qad_start_day,
    has_new_parenteral_in_window,
    CAST(gs AS DATE) AS covered_day
  FROM eligible_courses
  CROSS JOIN generate_series(course_start_day, course_end_day, INTERVAL 1 DAY) AS t(gs)
  WHERE CAST(gs AS DATE) BETWEEN qad_start_day AND (qad_start_day + INTERVAL 6 DAY)
),

/* 7) Initial consecutive run starting at qad_start_day */
run_calc AS (
  SELECT
    hospitalization_id,
    bc_id,
    culture_time,
    culture_day,
    qad_start_day,
    has_new_parenteral_in_window,
    covered_day,
    ROW_NUMBER() OVER (
      PARTITION BY hospitalization_id, bc_id
      ORDER BY covered_day
    ) AS rn,
    (covered_day - qad_start_day) AS day_offset
  FROM covered_days
  WHERE covered_day >= qad_start_day
),

initial_run AS (
  SELECT
    hospitalization_id,
    bc_id,
    culture_time,
    culture_day,
    qad_start_day,
    has_new_parenteral_in_window,
    COUNT(*) AS qad_days,
    MIN(covered_day) AS qad_run_start,
    MAX(covered_day) AS qad_run_end
  FROM run_calc
  WHERE (day_offset - (rn - 1)) = 0
  GROUP BY hospitalization_id, bc_id, culture_time, culture_day, qad_start_day, has_new_parenteral_in_window
)

/* Final output (one row per culture) */
SELECT
  ir.hospitalization_id,
  ir.bc_id,
  ir.culture_time,
  ir.culture_day,
  ir.qad_start_day,
  ir.qad_days,
  ir.qad_run_start,
  ir.qad_run_end,
  ir.has_new_parenteral_in_window,

  CASE
    WHEN ir.has_new_parenteral_in_window = 1 AND ir.qad_days >= 4 THEN 1
    ELSE 0
  END AS meets_qad_criteria,

  am.anchor_meds_in_window,
  am.anchor_parenteral_meds_in_window,
  rm.run_meds

FROM initial_run ir
LEFT JOIN qc_anchor_meds am
  ON ir.hospitalization_id = am.hospitalization_id
 AND ir.bc_id = am.bc_id
LEFT JOIN qc_run_meds rm
  ON ir.hospitalization_id = rm.hospitalization_id
 AND ir.bc_id = rm.bc_id
ORDER BY ir.hospitalization_id, ir.bc_id
"""

# QAD Censoring Query
QAD_CENSORING_QUERY = """
WITH qad_with_censor AS (
  SELECT
    q.*,

    -- From hospitalization
    h.discharge_dttm,
    h.discharge_category,

    -- From patient (death)
    p.death_dttm,

    -- Prefer hospitalization end; fall back to patient death only if discharge day is missing
    CASE
      WHEN h.discharge_dttm IS NOT NULL THEN h.discharge_dttm
      WHEN p.death_dttm IS NOT NULL THEN p.death_dttm
      ELSE NULL
    END AS censor_dttm,

    DATE(
      CASE
        WHEN h.discharge_dttm IS NOT NULL THEN h.discharge_dttm
        WHEN p.death_dttm IS NOT NULL THEN p.death_dttm
        ELSE NULL
      END
    ) AS censor_day,

    -- qualifying censoring categories (use exactly what your pipeline expects)
    CASE
      WHEN h.discharge_category IN (
        'expired', 'Expired',
        'acute_care_hospital', 'Acute Care Hospital',
        'hospice', 'Hospice'
      )
      THEN 1 ELSE 0
    END AS qualifies_for_censoring

  FROM qad_results q
  INNER JOIN hospitalizations h
    ON q.hospitalization_id = h.hospitalization_id
  LEFT JOIN patient p
    ON h.patient_id = p.patient_id
)

SELECT
  hospitalization_id,
  bc_id,
  culture_time,
  culture_day,

  qad_start_day,
  qad_days,
  qad_run_start,
  qad_run_end,

  discharge_dttm,
  discharge_category,
  death_dttm,
  censor_dttm,
  censor_day,
  qualifies_for_censoring,

  has_new_parenteral_in_window,
  meets_qad_criteria,

  anchor_meds_in_window,
  anchor_parenteral_meds_in_window,
  run_meds,

  CASE WHEN qad_run_end > censor_day THEN 1 ELSE 0 END AS run_extends_past_censor,

  CASE
    WHEN meets_qad_criteria = 1 THEN 1
    WHEN qad_days >= 1
      AND has_new_parenteral_in_window = 1
      AND qualifies_for_censoring = 1
      AND censor_dttm IS NOT NULL
      AND censor_day <= qad_start_day + INTERVAL 3 DAY
      AND qad_run_end >= censor_day - INTERVAL 1 DAY
    THEN 1
    ELSE 0
  END AS meets_qad_with_censoring,

  CASE
    WHEN meets_qad_criteria = 1
      THEN 'Meets QAD (standard)'
    WHEN qad_days >= 1
      AND has_new_parenteral_in_window = 1
      AND qualifies_for_censoring = 1
      AND censor_dttm IS NOT NULL
      AND censor_day <= qad_start_day + INTERVAL 3 DAY
      AND qad_run_end >= censor_day - INTERVAL 1 DAY
      THEN 'Meets QAD (censoring exception)'
    WHEN has_new_parenteral_in_window = 0
      THEN 'Fails QAD: no new IV/IM in window'
    ELSE 'Fails QAD: insufficient QAD days'
  END AS final_qad_status

FROM qad_with_censor
"""

# Lab Dysfunction Query
LAB_DYSFUNCTION_QUERY = f"""
      WITH
      bc_hosp AS (
        SELECT * FROM bc_episodes
      ),
      bc_hosp_ids AS (
        SELECT DISTINCT hospitalization_id FROM bc_hosp
      ),

      -- Filter labs early (performance), normalize value + timestamp, and apply outlier caps
      labs_filtered AS (
        SELECT
          l.hospitalization_id,
          l.lab_category,
          COALESCE(l.lab_value_numeric, TRY_CAST(l.lab_value AS DOUBLE)) AS value,
          COALESCE(l.lab_result_dttm, l.lab_order_dttm) AS lab_dttm
        FROM labs l
        WHERE l.hospitalization_id IN (SELECT hospitalization_id FROM bc_hosp_ids)
          AND l.lab_category IN ('creatinine','bilirubin_total','platelet_count','lactate')
          AND COALESCE(l.lab_value_numeric, TRY_CAST(l.lab_value AS DOUBLE)) IS NOT NULL
          AND COALESCE(l.lab_result_dttm, l.lab_order_dttm) IS NOT NULL
          AND (
            (l.lab_category = 'creatinine'      AND COALESCE(l.lab_value_numeric, TRY_CAST(l.lab_value AS DOUBLE)) <= {OUTLIERS['creatinine_max']})
            OR
            (l.lab_category = 'bilirubin_total' AND COALESCE(l.lab_value_numeric, TRY_CAST(l.lab_value AS DOUBLE)) <= {OUTLIERS['bilirubin_max']})
            OR
            (l.lab_category = 'platelet_count'  AND COALESCE(l.lab_value_numeric, TRY_CAST(l.lab_value AS DOUBLE)) <= {OUTLIERS['platelet_max']})
            OR
            (l.lab_category = 'lactate'         AND COALESCE(l.lab_value_numeric, TRY_CAST(l.lab_value AS DOUBLE)) <= {OUTLIERS['lactate_max']})
          )
      ),

      -- Community baselines: whole hospitalization
      baseline_community AS (
        SELECT
          hospitalization_id,
          MIN(CASE WHEN lab_category = 'creatinine'      THEN value END) AS cr_baseline_co,
          MIN(CASE WHEN lab_category = 'bilirubin_total' THEN value END) AS bili_baseline_co,
          MAX(CASE WHEN lab_category = 'platelet_count'  THEN value END) AS plt_baseline_raw_co,
          MAX(CASE WHEN lab_category = 'platelet_count' AND value >= 100 THEN 1 ELSE 0 END) AS plt_has_ge100_co
        FROM labs_filtered
        WHERE lab_category IN ('creatinine','bilirubin_total','platelet_count')
        GROUP BY hospitalization_id
      ),
      baseline_community_final AS (
        SELECT
          hospitalization_id,
          cr_baseline_co,
          bili_baseline_co,
          CASE WHEN plt_has_ge100_co = 1 THEN plt_baseline_raw_co ELSE NULL END AS plt_baseline_co
        FROM baseline_community
      ),

      -- Labs in the ±2 calendar-day window around blood culture day (per bc_id)
      labs_window AS (
        SELECT
          lf.hospitalization_id,
          bc.bc_id,
          lf.lab_category,
          lf.value,
          lf.lab_dttm,
          bc.blood_culture_day
        FROM labs_filtered lf
        JOIN bc_hosp bc
          ON lf.hospitalization_id = bc.hospitalization_id
        WHERE DATE(lf.lab_dttm) BETWEEN bc.blood_culture_day - INTERVAL '{WINDOW_DAYS} days'
                                  AND bc.blood_culture_day + INTERVAL '{WINDOW_DAYS} days'
      ),

      -- Hospital baselines: within ±2 days of blood culture day (per bc_id)
      baseline_hospital AS (
        SELECT
          hospitalization_id,
          bc_id,
          MIN(CASE WHEN lab_category = 'creatinine'      THEN value END) AS cr_baseline_ho,
          MIN(CASE WHEN lab_category = 'bilirubin_total' THEN value END) AS bili_baseline_ho,
          MAX(CASE WHEN lab_category = 'platelet_count' AND value >= 100 THEN value END) AS plt_baseline_ho
        FROM labs_window
        WHERE lab_category IN ('creatinine','bilirubin_total','platelet_count')
        GROUP BY hospitalization_id, bc_id
      ),

      -- ESRD flags (your table: esrd_patients has hospitalization_id, has_esrd=1)
      esrd_temp AS (
        SELECT hospitalization_id, 1 AS esrd
        FROM esrd_patients
      ),

      labs_with_baselines AS (
        SELECT
          lw.*,
          bc.cr_baseline_co,
          bc.bili_baseline_co,
          bc.plt_baseline_co,
          bh.cr_baseline_ho,
          bh.bili_baseline_ho,
          bh.plt_baseline_ho,
          e.esrd
        FROM labs_window lw
        LEFT JOIN baseline_community_final bc
          ON lw.hospitalization_id = bc.hospitalization_id
        LEFT JOIN baseline_hospital bh
          ON lw.hospitalization_id = bh.hospitalization_id AND lw.bc_id = bh.bc_id
        LEFT JOIN esrd_temp e
          ON lw.hospitalization_id = e.hospitalization_id
      ),

      -- AKI: creatinine >= 2x baseline, exclude ESRD
      aki AS (
        SELECT
          hospitalization_id,
          bc_id,
          MIN(CASE WHEN esrd IS NULL AND cr_baseline_co IS NOT NULL AND value >= 2.0 * cr_baseline_co THEN lab_dttm END) AS aki_dttm_co,
          MIN(CASE WHEN esrd IS NULL AND cr_baseline_ho IS NOT NULL AND value >= 2.0 * cr_baseline_ho THEN lab_dttm END) AS aki_dttm_ho
        FROM labs_with_baselines
        WHERE lab_category = 'creatinine'
        GROUP BY hospitalization_id, bc_id
      ),

      -- Hyperbilirubinemia: bili >=2.0 and relative increase vs baseline
      hyperbili AS (
        SELECT
          hospitalization_id,
          bc_id,
          MIN(CASE WHEN bili_baseline_co IS NOT NULL AND value >= 2.0 AND value >= {BILI_MULTIPLIER} * bili_baseline_co THEN lab_dttm END) AS hyperbili_dttm_co,
          MIN(CASE WHEN bili_baseline_ho IS NOT NULL AND value >= 2.0 AND value >= {BILI_MULTIPLIER} * bili_baseline_ho THEN lab_dttm END) AS hyperbili_dttm_ho
        FROM labs_with_baselines
        WHERE lab_category = 'bilirubin_total'
        GROUP BY hospitalization_id, bc_id
      ),

      -- Thrombocytopenia: value <100 and <= 0.5 * baseline, baseline must be usable (>=100 rule handled by baseline_* tables)
      thrombo AS (
        SELECT
          hospitalization_id,
          bc_id,
          MIN(CASE WHEN plt_baseline_co IS NOT NULL AND value < 100.0 AND value <= 0.5 * plt_baseline_co THEN lab_dttm END) AS thrombo_dttm_co,
          MIN(CASE WHEN plt_baseline_ho IS NOT NULL AND value < 100.0 AND value <= 0.5 * plt_baseline_ho THEN lab_dttm END) AS thrombo_dttm_ho
        FROM labs_with_baselines
        WHERE lab_category = 'platelet_count'
        GROUP BY hospitalization_id, bc_id
      ),

      -- Optional lactate >=2.0 (no baseline)
      lactate AS (
        SELECT
          hospitalization_id,
          bc_id,
          MIN(CASE WHEN value >= 2.0 THEN lab_dttm END) AS lactate_dttm
        FROM labs_with_baselines
        WHERE lab_category = 'lactate'
        GROUP BY hospitalization_id, bc_id
      )

      SELECT
        bc.hospitalization_id,
        bc.bc_id,
        bc.blood_culture_dttm,
        bc.blood_culture_day,
        bc.meets_qad_with_censoring,

        -- Baselines (QC)
        bco.cr_baseline_co,
        bco.bili_baseline_co,
        bco.plt_baseline_co,
        bho.cr_baseline_ho,
        bho.bili_baseline_ho,
        bho.plt_baseline_ho,

        -- ESRD exclusion flag (QC)
        CASE WHEN e.esrd IS NULL THEN 0 ELSE 1 END AS has_esrd,

        -- Dysfunction times under BOTH baseline scenarios
        a.aki_dttm_co,
        a.aki_dttm_ho,
        hb.hyperbili_dttm_co,
        hb.hyperbili_dttm_ho,
        t.thrombo_dttm_co,
        t.thrombo_dttm_ho,

        -- Optional
        lac.lactate_dttm

      FROM bc_hosp bc
      LEFT JOIN baseline_community_final bco
        ON bc.hospitalization_id = bco.hospitalization_id
      LEFT JOIN baseline_hospital bho
        ON bc.hospitalization_id = bho.hospitalization_id AND bc.bc_id = bho.bc_id
      LEFT JOIN esrd_temp e
        ON bc.hospitalization_id = e.hospitalization_id
      LEFT JOIN aki a
        ON bc.hospitalization_id = a.hospitalization_id AND bc.bc_id = a.bc_id
      LEFT JOIN hyperbili hb
        ON bc.hospitalization_id = hb.hospitalization_id AND bc.bc_id = hb.bc_id
      LEFT JOIN thrombo t
        ON bc.hospitalization_id = t.hospitalization_id AND bc.bc_id = t.bc_id
      LEFT JOIN lactate lac
        ON bc.hospitalization_id = lac.hospitalization_id AND bc.bc_id = lac.bc_id
      ORDER BY bc.hospitalization_id, bc.bc_id
      """

# Vasopressor Query
VASOPRESSOR_QUERY = """
WITH bc_hosp AS (
    SELECT * FROM blood_cultures_temp
),
-- First compute new initiation at the vasopressor level (before joining to BCs)
vaso_with_prev AS (
    SELECT
        m.hospitalization_id,
        m.admin_dttm,
        m.med_name,
        m.med_category,
        DATE(m.admin_dttm) AS admin_date,
        LAG(DATE(m.admin_dttm)) OVER (
            PARTITION BY m.hospitalization_id, m.med_category
            ORDER BY m.admin_dttm
        ) AS prev_admin_date
    FROM med_continuous m
    LEFT JOIN adt a
      ON m.hospitalization_id = a.hospitalization_id
     AND m.admin_dttm >= a.in_dttm
     AND m.admin_dttm <  a.out_dttm
    WHERE m.med_group = 'vasoactives'
      AND m.med_dose > 0
      AND (a.location_category IS NULL OR LOWER(a.location_category) != 'procedural')
),
-- Filter to new initiations only
new_vaso AS (
    SELECT *
    FROM vaso_with_prev
    WHERE prev_admin_date IS NULL OR DATEDIFF('day', prev_admin_date, admin_date) > 1
),
-- Now join to blood cultures and filter by window
new_vaso_in_window AS (
    SELECT
        v.hospitalization_id,
        bc.bc_id,
        v.admin_dttm,
        v.med_category,
        bc.blood_culture_dttm
    FROM new_vaso v
    JOIN bc_hosp bc
      ON v.hospitalization_id = bc.hospitalization_id
    WHERE v.admin_dttm BETWEEN
          bc.blood_culture_dttm - INTERVAL '2 days'
          AND bc.blood_culture_dttm + INTERVAL '2 days'
)
SELECT
    hospitalization_id,
    bc_id,
    MIN(admin_dttm) AS vasopressor_dttm,
    FIRST(med_category ORDER BY admin_dttm) AS vasopressor_name
FROM new_vaso_in_window
GROUP BY hospitalization_id, bc_id
"""

# IMV Query
IMV_QUERY = """
WITH bc_hosp AS (
    SELECT * FROM blood_cultures_temp
),
-- First compute new IMV episodes at the patient level (before joining to BCs)
imv_with_prev AS (
    SELECT
        r.hospitalization_id,
        r.recorded_dttm,
        DATE(r.recorded_dttm) AS imv_date,
        LAG(DATE(r.recorded_dttm)) OVER (
            PARTITION BY r.hospitalization_id
            ORDER BY r.recorded_dttm
        ) AS prev_imv_date
    FROM respiratory r
    WHERE LOWER(r.device_category) = 'imv'
),
-- Filter to new episodes only
new_imv AS (
    SELECT *
    FROM imv_with_prev
    WHERE prev_imv_date IS NULL OR DATEDIFF('day', prev_imv_date, imv_date) > 1
),
-- Now join to blood cultures and filter by window
new_imv_in_window AS (
    SELECT
        i.hospitalization_id,
        bc.bc_id,
        i.recorded_dttm,
        bc.blood_culture_dttm
    FROM new_imv i
    JOIN bc_hosp bc
      ON i.hospitalization_id = bc.hospitalization_id
    WHERE i.recorded_dttm BETWEEN
          bc.blood_culture_dttm - INTERVAL '2 days'
          AND bc.blood_culture_dttm + INTERVAL '2 days'
)
SELECT
    hospitalization_id,
    bc_id,
    MIN(recorded_dttm) AS imv_dttm
FROM new_imv_in_window
GROUP BY hospitalization_id, bc_id
"""

# ==============================================================================
# 4. MEMORY MANAGEMENT UTILITIES - MODIFIED FOR CLIFPY
# ==============================================================================

def load_and_register(
    table_class,
    config: Dict,
    filters: Dict,
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: Optional[List[str]] = None,
    verbose: bool = False
) -> None:
    """
    Load CLIF table using clifpy API, register with DuckDB, and cleanup.
    """
    if verbose:
        print(f"  Loading {table_name}...")

    # Extract config parameters for clifpy
    data_directory = config.get('tables_path') or config.get('data_directory')
    filetype = config.get('file_type') or config.get('filetype') 
    timezone = config.get('timezone') 

    # Load using clifpy table API
    if columns:
        df = table_class.from_file(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            filters=filters,
            columns=columns
        ).df
    else:
        df = table_class.from_file(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            filters=filters
        ).df

    # Register with DuckDB
    con.register(table_name, df)

    # Get row count for logging
    row_count = len(df)
    if verbose:
        print(f"    Loaded {row_count:,} rows, memory cleaned")
    return df

def drop_tables(con: duckdb.DuckDBPyConnection, table_names: List[str]) -> None:
    """Drop DuckDB tables or views to free memory."""
    for table in table_names:
        # Try dropping as view first (most common from register), then as table
        try:
            con.execute(f"DROP VIEW IF EXISTS {table}")
        except:
            pass
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
        except:
            pass

# ==============================================================================
# 5. COMPONENT A: PRESUMED INFECTION (BLOOD CULTURES & QAD)
# ==============================================================================

def process_blood_cultures(
    con: duckdb.DuckDBPyConnection,
    hospitalization_ids: List[str],
    config: Dict,
    verbose: bool = False
) -> None:
    """
    Identify blood cultures for hospitalizations.
    Creates: blood_cultures table in DuckDB
    """
    from clifpy.tables import MicrobiologyCulture
    
    if verbose:
        print("\n=== Processing Blood Cultures ===")

    # Load microbiology cultures
    culture_df = load_and_register(
        MicrobiologyCulture,
        config,
        filters={'hospitalization_id': hospitalization_ids,
                'fluid_category': ['blood_buffy']},
        con=con,
        table_name='cultures',
        verbose=verbose
    )

    # Create culture time column (use collect time, fallback to order time)
    # culture_df['culture_time'] = culture_df['collect_dttm'].fillna(culture_df['order_dttm'])
    culture_df['culture_time'] = culture_df['collect_dttm']
    culture_df['culture_day'] = pd.to_datetime(culture_df['culture_time']).dt.date
    # Assign unique ID to EACH blood culture per hospitalization
    # This allows us to evaluate each blood culture independently per CDC guidelines
    culture_df = culture_df.sort_values(['hospitalization_id', 'culture_time'])
    culture_df = culture_df.groupby(['hospitalization_id', 'culture_time']).first().reset_index()
    culture_df['bc_id'] = culture_df.groupby('hospitalization_id').cumcount() + 1

    if verbose:
        print(f"Total cultures loaded: {len(culture_df)}")
        print(f"Fluid categories: {culture_df['fluid_category'].value_counts().to_dict()}")
        print(f"\nBlood cultures per patient statistics:")
        bc_per_patient = culture_df.groupby('hospitalization_id')['bc_id'].max()
        print(f"  Patients with 1 blood culture: {(bc_per_patient == 1).sum()}")
        print(f"  Patients with 2+ blood cultures: {(bc_per_patient > 1).sum()}")
        print(f"  Max blood cultures per patient: {bc_per_patient.max()}")

    # Register with DuckDB
    con.register("blood_cultures", culture_df)
    # Cleanup
    drop_tables(con, ['cultures'])
    del culture_df


def calculate_qad(
    con: duckdb.DuckDBPyConnection,
    hospitalization_ids: List[str],
    config: Dict,
    verbose: bool = False
) -> None:
    """
    Calculate Qualifying Antimicrobial Days.
    Creates: final_qad table in DuckDB
    """
    from clifpy.tables import MedicationAdminIntermittent, Hospitalization, Patient
    
    if verbose:
        print("\n=== Calculating QAD ===")

    # Load required tables
    med_int_df = load_and_register(
        MedicationAdminIntermittent,
        config,
        filters={'hospitalization_id': hospitalization_ids,
                 'med_group': ['CMS_sepsis_qualifying_antibiotics', 'cms_sepsis_qualifying_antibiotics']},
        con=con,
        table_name='antibiotics_raw',
        verbose=verbose
    )

    # Standardize antibiotic data
    med_int_df['med_admin_time'] = med_int_df['admin_dttm']
    med_int_df['med_admin_day'] = pd.to_datetime(med_int_df['med_admin_time']).dt.date

    # Identify IV/IM routes
    iv_im_routes = ['iv', 'im', 'intravenous', 'intramuscular']
    med_int_df['med_route_category_lower'] = med_int_df['med_route_category'].str.lower()
    med_int_df['is_iv_im'] = med_int_df['med_route_category_lower'].isin(iv_im_routes).astype(int)

    # Register with DuckDB using consistent name
    con.register("antibiotics", med_int_df)
    # Cleanup
    drop_tables(con, ['antibiotics_raw'])
    del med_int_df

    hosp_df = load_and_register(
        Hospitalization,
        config,
        filters={'hospitalization_id': hospitalization_ids},
        con=con,
        table_name='hospitalizations',
        verbose=verbose
    )
    # Get patient IDs
    patient_ids = con.execute("SELECT DISTINCT patient_id FROM hospitalizations").fetchdf()['patient_id'].tolist()

    patient_df = load_and_register(
        Patient,
        config,
        filters={'patient_id': patient_ids},
        con=con,
        table_name='patient',
        columns=['patient_id', 'death_dttm'],
        verbose=verbose
    )

    # Execute QAD calculation (original query preserved)
    con.execute("CREATE TABLE qad_results AS " + QAD_QUERY)

    # Apply censoring rules
    con.execute("CREATE TABLE final_qad AS " + QAD_CENSORING_QUERY)

    # Create bc_episodes view for downstream queries
    con.execute("""
        CREATE TABLE bc_episodes AS
        SELECT
            bc.hospitalization_id,
            bc.bc_id,
            bc.culture_time AS blood_culture_dttm,
            bc.culture_day AS blood_culture_day,
            -- Component A: if no QAD row exists, presumed infection should be 0 (not dropped)
            COALESCE(q.meets_qad_with_censoring, 0) AS meets_qad_with_censoring,
            -- QC columns: will be NULL for episodes with no QAD anchor
            q.anchor_meds_in_window,
            q.anchor_parenteral_meds_in_window,
            q.run_meds
        FROM blood_cultures bc
        LEFT JOIN final_qad q
            ON bc.hospitalization_id = q.hospitalization_id
            AND bc.bc_id = q.bc_id
        WHERE bc.culture_time IS NOT NULL
    """)

    #cleanup
    del hosp_df
    del patient_df
    drop_tables(con, ['qad_results'])
    if verbose:
        qad_count = con.execute("SELECT COUNT(*) FROM final_qad WHERE meets_qad_with_censoring = 1").fetchone()[0]
        print(f"  Found {qad_count} blood cultures with ≥4 QAD")

# ==============================================================================
# 6. COMPONENT B: ORGAN DYSFUNCTION
# ==============================================================================

def calculate_lab_dysfunction(
    con: duckdb.DuckDBPyConnection,
    hospitalization_ids: List[str],
    config: Dict,
    include_lactate: bool = False,
    verbose: bool = False
) -> None:
    """
    Calculate laboratory-based organ dysfunction.
    """
    from clifpy.tables import Labs, HospitalDiagnosis
    
    if verbose:
        print("\n=== Calculating Lab Dysfunction ===")

    # Load labs
    labs_df = load_and_register(
        Labs,
        config,
        filters={'hospitalization_id': hospitalization_ids},
        con=con,
        table_name='labs',
        columns=[
            "hospitalization_id",
            "lab_category",
            "lab_value",
            "lab_value_numeric",
            "lab_result_dttm",
            "lab_order_dttm",
        ],
        verbose=verbose
    )

    # Process labs with outlier filtering
    con.execute(f"""
        CREATE TABLE labs_filtered AS
        SELECT
            hospitalization_id,
            lab_result_dttm AS lab_dttm,
            lab_result_dttm,
            lab_category,
            lab_value_numeric AS value
        FROM labs
        WHERE lab_value_numeric IS NOT NULL
        AND (
            (lab_category = 'creatinine' AND lab_value_numeric <= {OUTLIERS['creatinine_max']})
            OR (lab_category = 'bilirubin_total' AND lab_value_numeric <= {OUTLIERS['bilirubin_max']})
            OR (lab_category = 'platelet_count' AND lab_value_numeric <= {OUTLIERS['platelet_max']})
            OR (lab_category = 'lactate' AND lab_value_numeric <= {OUTLIERS['lactate_max']})
            OR lab_category NOT IN ('creatinine', 'bilirubin_total', 'platelet_count', 'lactate')
        )
    """)

    con.execute("CREATE TABLE labs AS SELECT * FROM labs_filtered")

    # Load diagnoses for ESRD exclusion
    diagnosis_df = load_and_register(
        HospitalDiagnosis,
        config,
        filters={'hospitalization_id': hospitalization_ids},
        con=con,
        table_name='diagnoses',
        verbose=verbose
    )

    # Calculate ESRD exclusions
    # Clean diagnosis_code: lowercase, strip whitespace, remove dots if present
    diagnosis_df['diagnosis_code_clean'] = (
        diagnosis_df['diagnosis_code']
        .astype(str)
        .str.lower()
        .str.strip()
        .str.replace('.', '', regex=False)
    )

    # Identify ESRD patients by the above code list
    esrd_patients = (
        diagnosis_df[
            diagnosis_df['diagnosis_code_clean'].isin(ESRD_CODES)
        ][['hospitalization_id']].drop_duplicates()
    )
    esrd_patients['has_esrd'] = 1
    con.register("esrd_patients", esrd_patients)
    # Prepare lactate selection based on include_lactate flag
    if include_lactate:
        lactate_select = "MIN(CASE WHEN value >= 2.0 THEN lab_dttm END) AS lactate_dttm"
    else:
        lactate_select = "NULL AS lactate_dttm"

    # Execute lab dysfunction query
    lab_query = LAB_DYSFUNCTION_QUERY.replace("{WINDOW_DAYS}", str(WINDOW_DAYS))
    lab_query = lab_query.replace("{BILI_MULTIPLIER}", str(BILI_MULTIPLIER))
    lab_query = lab_query.replace("{lactate_select}", lactate_select)

    # Replace OUTLIERS placeholders
    lab_query = lab_query.replace("{OUTLIERS['creatinine_max']}", str(OUTLIERS['creatinine_max']))
    lab_query = lab_query.replace("{OUTLIERS['bilirubin_max']}", str(OUTLIERS['bilirubin_max']))
    lab_query = lab_query.replace("{OUTLIERS['platelet_max']}", str(OUTLIERS['platelet_max']))
    lab_query = lab_query.replace("{OUTLIERS['lactate_max']}", str(OUTLIERS['lactate_max']))

    con.execute("CREATE TABLE lab_dysfunction AS " + lab_query)

    # Cleanup
    drop_tables(con, [ 'diagnoses'])
    drop_tables(con, ['labs_filtered', 'baseline_community', 'baseline_hospital'])
    del diagnosis_df
    del labs_df


def calculate_clinical_interventions(
    con: duckdb.DuckDBPyConnection,
    hospitalization_ids: List[str],
    config: Dict,
    verbose: bool = False
) -> None:
    """
    Calculate vasopressor and mechanical ventilation criteria.
    """
    from clifpy.tables import MedicationAdminContinuous, Adt, RespiratorySupport
    
    if verbose:
        print("\n=== Calculating Clinical Interventions ===")

    # Create temp view for blood cultures
    con.execute("""
        CREATE OR REPLACE TEMP VIEW blood_cultures_temp AS
        SELECT
          hospitalization_id,
          bc_id,
          culture_time AS blood_culture_dttm
        FROM blood_cultures
        WHERE culture_time IS NOT NULL
    """)

    # Load medications for vasopressors
    med_cont_df = load_and_register(
        MedicationAdminContinuous,
        config,
        filters={'hospitalization_id': hospitalization_ids,
                 'med_group': ['vasoactives']},
        con=con,
        table_name='med_continuous',
        verbose=verbose
    )

    # Load ADT for location filtering
    adt_df = load_and_register(
        Adt,
        config,
        filters={'hospitalization_id': hospitalization_ids},
        con=con,
        table_name='adt',
        columns=['hospitalization_id', 'in_dttm', 'out_dttm', 'location_category'],
        verbose=verbose
    )

    # Execute vasopressor query
    con.execute("CREATE TABLE vasopressor_df AS " + VASOPRESSOR_QUERY)

    # Load respiratory support
    resp_df = load_and_register(
        RespiratorySupport,
        config,
        filters={'hospitalization_id': hospitalization_ids},
        con=con,
        table_name='respiratory',
        verbose=verbose
    )

    # Execute IMV query
    con.execute("CREATE TABLE imv_df AS " + IMV_QUERY)

    # Cleanup
    # drop_tables(con, ['med_continuous', 'adt', 'respiratory'])
    del med_cont_df
    del resp_df
    del adt_df

    if verbose:
        vaso_count = con.execute("SELECT COUNT(*) FROM vasopressor_df").fetchone()[0]
        imv_count = con.execute("SELECT COUNT(*) FROM imv_df").fetchone()[0]
        print(f"  Found {vaso_count} vasopressor and {imv_count} IMV events")

# ==============================================================================
# 7. ASE CALCULATION AND RIT PROCESSING
# ==============================================================================

def combine_components_for_ase(
    con: duckdb.DuckDBPyConnection,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Combine Component A and B to determine ASE episodes.
    Returns DataFrame with all ASE calculations.
    """
    if verbose:
        print("\n=== Determining ASE Episodes ===")

    # Create component_b_inputs combining all data
    con.execute("""
        CREATE TEMP TABLE component_b_inputs AS
        WITH base AS (
          SELECT
            bc.hospitalization_id,
            bc.bc_id,
            bc.blood_culture_dttm,
            bc.blood_culture_day,
            h.admission_dttm,

            -- Component A
            bc.meets_qad_with_censoring AS presumed_infection,

            -- QAD columns
            q.qad_days AS total_qad,
            q.qad_run_start AS qad_start_date,
            q.qad_run_end AS qad_end_date,
            q.final_qad_status,

            -- QAD anchor day (treat as day-level; only count if there was a new parenteral start in window)
            CASE
              WHEN q.has_new_parenteral_in_window = 1 AND q.qad_start_day IS NOT NULL
                THEN CAST(q.qad_start_day AS TIMESTAMP)
              ELSE NULL
            END AS first_qad_dttm,

            -- Keep QC columns
            bc.anchor_meds_in_window,
            bc.anchor_parenteral_meds_in_window,
            bc.run_meds,

            -- "Type for baseline selection"
            CASE
              WHEN DATEDIFF('day', DATE(h.admission_dttm), DATE(bc.blood_culture_dttm)) + 1 <= 2
                THEN 'community'
              ELSE 'hospital'
            END AS type_for_baseline
          FROM bc_episodes bc
          JOIN hospitalizations h
            ON bc.hospitalization_id = h.hospitalization_id

          -- bring in the QAD-level columns from final_qad
          LEFT JOIN final_qad q
            ON bc.hospitalization_id = q.hospitalization_id
          AND bc.bc_id = q.bc_id
        ),

        organ_nonlab AS (
          SELECT
            b.*,
            v.vasopressor_dttm,
            v.vasopressor_name,
            i.imv_dttm
          FROM base b
          LEFT JOIN vasopressor_df v
            ON b.hospitalization_id = v.hospitalization_id
          AND b.bc_id = v.bc_id
          LEFT JOIN imv_df i
            ON b.hospitalization_id = i.hospitalization_id
          AND b.bc_id = i.bc_id
        ),

        organ_labs AS (
          SELECT
            o.*,

            -- pick the baseline scenario using type_for_baseline
            CASE WHEN o.type_for_baseline = 'community' THEN ld.aki_dttm_co              ELSE ld.aki_dttm_ho              END AS aki_dttm,
    CASE WHEN o.type_for_baseline = 'community' THEN ld.hyperbili_dttm_co        ELSE ld.hyperbili_dttm_ho        END AS hyperbilirubinemia_dttm,
    CASE WHEN o.type_for_baseline = 'community' THEN ld.thrombo_dttm_co          ELSE ld.thrombo_dttm_ho          END AS thrombocytopenia_dttm,

        -- optional lactate (no baseline)
        ld.lactate_dttm,
        ld.has_esrd
      FROM organ_nonlab o
      LEFT JOIN lab_dysfunction ld
        ON o.hospitalization_id = ld.hospitalization_id
      AND o.bc_id = ld.bc_id
    )

    SELECT * FROM organ_labs
    """)

    # Calculate ASE with all component combinations
    ase_query = """
        WITH x AS (
          SELECT
            c.*
          FROM component_b_inputs c
        ),

        y AS (
          SELECT
            x.*,

            -- organ dysfunction flags
            (
              vasopressor_dttm IS NOT NULL OR imv_dttm IS NOT NULL OR
              aki_dttm IS NOT NULL OR hyperbilirubinemia_dttm IS NOT NULL OR
              thrombocytopenia_dttm IS NOT NULL OR lactate_dttm IS NOT NULL
            ) AS has_organ_dysfunction_w_lactate,

            (
              vasopressor_dttm IS NOT NULL OR imv_dttm IS NOT NULL OR
              aki_dttm IS NOT NULL OR hyperbilirubinemia_dttm IS NOT NULL OR
              thrombocytopenia_dttm IS NOT NULL
            ) AS has_organ_dysfunction_wo_lactate,

            -- sepsis (ASE) flags
            CASE WHEN presumed_infection = 1 AND (
              vasopressor_dttm IS NOT NULL OR imv_dttm IS NOT NULL OR
              aki_dttm IS NOT NULL OR hyperbilirubinemia_dttm IS NOT NULL OR
              thrombocytopenia_dttm IS NOT NULL OR lactate_dttm IS NOT NULL
            ) THEN 1 ELSE 0 END AS sepsis,

            CASE WHEN presumed_infection = 1 AND (
              vasopressor_dttm IS NOT NULL OR imv_dttm IS NOT NULL OR
              aki_dttm IS NOT NULL OR hyperbilirubinemia_dttm IS NOT NULL OR
              thrombocytopenia_dttm IS NOT NULL
            ) THEN 1 ELSE 0 END AS sepsis_wo_lactate,

            -- presumed infection onset (earliest of blood culture + first QAD), only if presumed_infection==1
            CASE
              WHEN presumed_infection = 1 THEN
                NULLIF(
                  LEAST(
                    COALESCE(blood_culture_dttm, TIMESTAMP '9999-12-31'),  -- Use original with time
                    COALESCE(first_qad_dttm,     TIMESTAMP '9999-12-31')   -- This will be midnight
                  ),
                  TIMESTAMP '9999-12-31'
                )
              ELSE NULL
            END AS presumed_infection_onset_dttm
          FROM x
        ),

        z AS (
          SELECT
            y.*,
            -- ASE onset (WITH lactate): USE ORIGINAL DATETIME COLUMNS
            NULLIF(
              LEAST(
                COALESCE(blood_culture_dttm, TIMESTAMP '9999-12-31'),     -- Original with time
                COALESCE(first_qad_dttm,     TIMESTAMP '9999-12-31'),     -- This will still be midnight (QAD is day-level)
                COALESCE(vasopressor_dttm,   TIMESTAMP '9999-12-31'),     -- Original with time
                COALESCE(imv_dttm,           TIMESTAMP '9999-12-31'),     -- Original with time
                COALESCE(aki_dttm,           TIMESTAMP '9999-12-31'),     -- Original with time
                COALESCE(hyperbilirubinemia_dttm, TIMESTAMP '9999-12-31'), -- Original with time
                COALESCE(thrombocytopenia_dttm,   TIMESTAMP '9999-12-31'), -- Original with time
                COALESCE(lactate_dttm,       TIMESTAMP '9999-12-31')      -- Original with time
              ),
              TIMESTAMP '9999-12-31'
            ) AS ase_onset_w_lactate_dttm,

            -- ASE onset (WITHOUT lactate): USE ORIGINAL DATETIME COLUMNS
            NULLIF(
              LEAST(
                COALESCE(blood_culture_dttm, TIMESTAMP '9999-12-31'),
                COALESCE(first_qad_dttm,     TIMESTAMP '9999-12-31'),
                COALESCE(vasopressor_dttm,   TIMESTAMP '9999-12-31'),
                COALESCE(imv_dttm,           TIMESTAMP '9999-12-31'),
                COALESCE(aki_dttm,           TIMESTAMP '9999-12-31'),
                COALESCE(hyperbilirubinemia_dttm, TIMESTAMP '9999-12-31'),
                COALESCE(thrombocytopenia_dttm,   TIMESTAMP '9999-12-31')
              ),
              TIMESTAMP '9999-12-31'
            ) AS ase_onset_wo_lactate_dttm
          FROM y
        ),

        w AS (
          SELECT
            z.*,

            -- First criterion (w/ lactate): which dttm matches the pre-computed onset?
            CASE
              WHEN ase_onset_w_lactate_dttm IS NULL THEN NULL
              WHEN blood_culture_dttm = ase_onset_w_lactate_dttm THEN 'blood_culture'
              WHEN first_qad_dttm = ase_onset_w_lactate_dttm THEN 'first_qad'
              WHEN vasopressor_dttm = ase_onset_w_lactate_dttm THEN 'vasopressor'
              WHEN imv_dttm = ase_onset_w_lactate_dttm THEN 'imv'
              WHEN aki_dttm = ase_onset_w_lactate_dttm THEN 'aki'
              WHEN hyperbilirubinemia_dttm = ase_onset_w_lactate_dttm THEN 'hyperbilirubinemia'
              WHEN thrombocytopenia_dttm = ase_onset_w_lactate_dttm THEN 'thrombocytopenia'
              WHEN lactate_dttm = ase_onset_w_lactate_dttm THEN 'lactate'
              ELSE NULL
            END AS ase_first_criteria_w_lactate,

            -- First criterion (w/o lactate): same logic, no lactate
            CASE
              WHEN ase_onset_wo_lactate_dttm IS NULL THEN NULL
              WHEN blood_culture_dttm = ase_onset_wo_lactate_dttm THEN 'blood_culture'
              WHEN first_qad_dttm = ase_onset_wo_lactate_dttm THEN 'first_qad'
              WHEN vasopressor_dttm = ase_onset_wo_lactate_dttm THEN 'vasopressor'
              WHEN imv_dttm = ase_onset_wo_lactate_dttm THEN 'imv'
              WHEN aki_dttm = ase_onset_wo_lactate_dttm THEN 'aki'
              WHEN hyperbilirubinemia_dttm = ase_onset_wo_lactate_dttm THEN 'hyperbilirubinemia'
              WHEN thrombocytopenia_dttm = ase_onset_wo_lactate_dttm THEN 'thrombocytopenia'
              ELSE NULL
            END AS ase_first_criteria_wo_lactate
          FROM z
        )

        SELECT
          hospitalization_id,
          bc_id,

          presumed_infection,
          sepsis,
          sepsis_wo_lactate,

          -- onset + type
          ase_onset_w_lactate_dttm,
          ase_first_criteria_w_lactate,
          ase_onset_wo_lactate_dttm,
          ase_first_criteria_wo_lactate,
          presumed_infection_onset_dttm,

          -- Community vs hospital onset type (based on onset day; admission day=day 1)
          CASE
            WHEN ase_onset_w_lactate_dttm IS NULL OR admission_dttm IS NULL THEN NULL
            WHEN DATEDIFF('day', DATE(admission_dttm), DATE(ase_onset_w_lactate_dttm)) + 1 <= 2 THEN 'community'
            ELSE 'hospital'
          END AS type,

          -- criteria timestamps (for QC)
          blood_culture_dttm,
          first_qad_dttm,
          vasopressor_dttm,
          vasopressor_name,
          imv_dttm,
          aki_dttm,
          hyperbilirubinemia_dttm,
          thrombocytopenia_dttm,
          lactate_dttm,

          -- flags
          has_organ_dysfunction_w_lactate,
          has_organ_dysfunction_wo_lactate,
          has_esrd,

          -- keep Component A QC columns
          anchor_meds_in_window,
          anchor_parenteral_meds_in_window,
          run_meds,
          final_qad_status,
          -- note: the baseline-selection helper used for lab picking
          type_for_baseline

        FROM w
        ORDER BY hospitalization_id, bc_id
    """

    component_b_df = con.execute(ase_query).df()
    qad_summary = con.execute("""
              SELECT
                  hospitalization_id,
                  bc_id,
                  qad_days     AS total_qad,
                  qad_run_start AS qad_start_date,
                  qad_run_end   AS qad_end_date
              FROM final_qad
          """).fetchdf()
    ase_df = component_b_df.merge(
                      qad_summary,
                      on=["hospitalization_id", "bc_id"],
                      how="left",
                  )
    # ---- 5b) Add no_sepsis_reason ----
    # Logic: if sepsis==1 => NA
    #        else if presumed_infection==0 => "no_presumed_infection"
    #        else => "no_organ_dysfunction"
    ase_df["no_sepsis_reason"] = pd.NA
    ase_df.loc[ase_df["sepsis"] != 1, "no_sepsis_reason"] = np.where(
        ase_df.loc[ase_df["sepsis"] != 1, "presumed_infection"].fillna(0).astype(int) == 0,
        "no_presumed_infection",
        "no_organ_dysfunction",
    )

    # ---- 5c) Ensure datetime columns are parsed (defensive) ----
    _dt_cols = [
        "blood_culture_dttm",
        "first_qad_dttm",
        "presumed_infection_onset_dttm",
        "ase_onset_w_lactate_dttm",
        "ase_onset_wo_lactate_dttm",
        "vasopressor_dttm",
        "imv_dttm",
        "aki_dttm",
        "hyperbilirubinemia_dttm",
        "thrombocytopenia_dttm",
        "lactate_dttm",
    ]
    for c in _dt_cols:
        if c in ase_df.columns:
            ase_df[c] = pd.to_datetime(ase_df[c], errors="coerce")

    if verbose:
        ase_count = len(ase_df[ase_df['sepsis'] == 1])
        print(f"  Found {ase_count} ASE episodes (before RIT)")

    return ase_df

def apply_rit_post_processing(
    df: pd.DataFrame,
    rit_days: int = 14,
    only_hospital_onset: bool = True
    ) -> pd.DataFrame:
    """
    Apply Repeat Infection Timeframe (RIT) post-processing.
    """
    df = df.copy()

    all_sepsis = df[df["sepsis"] == 1].copy()
    non_sepsis = df[df["sepsis"] != 1].copy()
    non_sepsis["episode_id"] = pd.NA

    if len(all_sepsis) == 0:
        return pd.concat([non_sepsis], ignore_index=True)

    all_sepsis = all_sepsis.sort_values(["hospitalization_id", "ase_onset_w_lactate_dttm", "bc_id"])

    # Vectorized: compute days since previous onset within hospitalization
    all_sepsis["prev_onset"] = all_sepsis.groupby("hospitalization_id")["ase_onset_w_lactate_dttm"].shift(1)
    all_sepsis["days_since_prev"] = (all_sepsis["ase_onset_w_lactate_dttm"] - all_sepsis["prev_onset"]).dt.days

    # Keep if: community-onset OR first in hospitalization OR >14 days since last
    all_sepsis["keep"] = (
        (all_sepsis["type"] != "hospital") |  # Community always kept
        (all_sepsis["prev_onset"].isna()) |    # First episode
        (all_sepsis["days_since_prev"] > rit_days)  # >14 days gap
    )

    # For hospital-onset, need cumulative tracking - use iterative marking
    # Actually need more complex logic for "any prior" not just "immediate prior"
    # Fall back to groupby().apply() which is faster than iterrows()

    def filter_group(g):
        g = g.sort_values(["ase_onset_w_lactate_dttm", "bc_id"])
        keep_mask = []
        last_onset = None
        for _, row in g.iterrows():
            onset = row["ase_onset_w_lactate_dttm"]
            is_hospital = row["type"] == "hospital"
            if pd.isna(onset) or not is_hospital:
                keep_mask.append(True)
                if not pd.isna(onset):
                    last_onset = onset
            else:
                if last_onset is None or (onset - last_onset).days > rit_days:
                    keep_mask.append(True)
                    last_onset = onset
                else:
                    keep_mask.append(False)
        return pd.Series(keep_mask, index=g.index)

    keep_mask = all_sepsis.groupby("hospitalization_id", group_keys=False).apply(filter_group)
    sepsis_filtered = all_sepsis[keep_mask].copy()

    # Combine back
    combined = pd.concat([sepsis_filtered, non_sepsis], ignore_index=True)

    # Assign episode_id for ALL sepsis==1 rows (after RIT removal)
    combined = combined.sort_values(["hospitalization_id", "ase_onset_w_lactate_dttm", "bc_id"]).reset_index(drop=True)
    combined["episode_id"] = pd.NA
    mask = combined["sepsis"] == 1
    combined.loc[mask, "episode_id"] = (
        combined.loc[mask].groupby("hospitalization_id", sort=False).cumcount() + 1
    )
    combined["episode_id"] = combined["episode_id"].astype("Int64")

    # Put episode_id immediately after bc_id
    cols = list(combined.columns)
    if "episode_id" in cols and "bc_id" in cols:
        cols.remove("episode_id")
        bc_idx = cols.index("bc_id")
        cols.insert(bc_idx + 1, "episode_id")
        combined = combined[cols]

    return combined

# ==============================================================================
# 8. MAIN COMPUTE FUNCTION - MODIFIED FOR CLIFPY INTEGRATION
# ==============================================================================

def compute_ase(
    hospitalization_ids: Union[List[str], None] = None,
    config_path: Union[str, Path, None] = None,
    data_directory: Optional[str] = None,
    filetype: str = 'parquet',
    timezone: str = 'UTC',
    apply_rit: bool = True,
    rit_only_hospital_onset: bool = True,
    include_lactate: bool = False,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Compute CDC Adult Sepsis Event (ASE) for given hospitalizations.

    Memory-efficient implementation with staged processing.

    Parameters
    ----------
    hospitalization_ids : list, optional
        List of hospitalization IDs to process. If None, processes all available.
    config_path : str or Path, optional
        Path to CLIF configuration file
    data_directory : str, optional
        Path to CLIF data directory (overrides config if provided)
    filetype : str, default='parquet'
        File format of CLIF data ('parquet' or 'csv')
    timezone : str, default='UTC'
        Timezone for datetime handling
    apply_rit : bool, default=True
        Whether to apply Repeat Infection Timeframe (14-day) filtering
    rit_only_hospital_onset : bool, default=True
        Apply RIT only to hospital-onset events (day ≥3)
    include_lactate : bool, default=False
        Include lactate ≥2.0 mmol/L as organ dysfunction criterion
    verbose : bool, default=True
        Show detailed progress messages and memory usage

    Returns
    -------
    pd.DataFrame
        ASE results with all required columns
    """
    from clifpy.tables import Hospitalization
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"ASE COMPUTATION STARTED")
        print(f"Processing {len(hospitalization_ids) if hospitalization_ids else 'all'} hospitalizations")
        print(f"Configuration: RIT={apply_rit}, Lactate={include_lactate}")
        print(f"{'='*60}")

    # Load configuration - MODIFIED FOR CLIFPY
    config = {}
    if config_path:
        config_path = Path(config_path) if isinstance(config_path, str) else config_path
        with open(config_path, encoding='utf-8') as f:
            config = json.load(f)
        # Extract parameters from config
        data_directory = data_directory or config.get('data_directory') or config.get('tables_path')
        filetype = config.get('filetype', config.get('file_type', filetype))
        timezone = config.get('timezone', timezone)

    if not data_directory:
        raise ValueError("data_directory must be provided either directly or via config_path")

    # Build config dict for internal use
    config = {
        'data_directory': data_directory,
        'tables_path': data_directory,
        'filetype': filetype,
        'file_type': filetype,
        'timezone': timezone
    }

    # Initialize DuckDB
    con = duckdb.connect(':memory:')
    con.execute(f"SET TimeZone = '{timezone}'")

    try:
        # If no hospitalization IDs provided, load all
        if hospitalization_ids is None:
            hosp_all = load_and_register(
                Hospitalization,
                config,
                filters={},
                con=con,
                table_name='hosp_all',
                columns=['hospitalization_id'],
                verbose=verbose
            )
            hospitalization_ids = hosp_all['hospitalization_id'].unique().tolist()
            drop_tables(con, ['hosp_all'])
            del hosp_all

        # Component A: Presumed Infection
        process_blood_cultures(con, hospitalization_ids, config, verbose)
        calculate_qad(con, hospitalization_ids, config, verbose)
        gc.collect()
        # Component B: Organ Dysfunction
        calculate_lab_dysfunction(con, hospitalization_ids, config, include_lactate, verbose)
        calculate_clinical_interventions(con, hospitalization_ids, config, verbose)
        gc.collect()
        # ASE Determination
        ase_df = combine_components_for_ase(con, verbose)
        gc.collect()
        # Apply RIT if requested
        if apply_rit:
            if verbose:
                print("\n=== Applying RIT Filter ===")
            ase_df = apply_rit_post_processing(
                ase_df,
                rit_days=RIT_DAYS,
                only_hospital_onset=rit_only_hospital_onset
            )
            if verbose:
                sepsis_count = ase_df['sepsis'].sum()
                print(f"  ASE episodes after RIT: {sepsis_count}")
        else:
            ase_df["episode_id"] = pd.NA
            ase_df["episode_id"] = ase_df["episode_id"].astype("Int64")

        # Add hospitalizations without blood cultures
        no_bc_hosps = set(hospitalization_ids) - set(ase_df['hospitalization_id'].unique())
        if no_bc_hosps:
            # Create temp table for non-BC hospitalizations
            no_bc_hosp_list = list(no_bc_hosps)
            no_bc_hosp_df = pd.DataFrame({'hospitalization_id': no_bc_hosp_list})
            con.register("no_bc_hosps", no_bc_hosp_df)

            # Query clinical data for non-BC patients
            no_bc_clinical = con.execute("""
                -- Vasopressor (first new initiation, excluding OR)
                WITH vaso AS (
                    SELECT
                        m.hospitalization_id,
                        MIN(m.admin_dttm) AS vasopressor_dttm,
                        FIRST(m.med_category ORDER BY m.admin_dttm) AS vasopressor_name
                    FROM med_continuous m
                    LEFT JOIN adt a ON m.hospitalization_id = a.hospitalization_id
                        AND m.admin_dttm >= a.in_dttm AND m.admin_dttm < a.out_dttm
                    WHERE m.hospitalization_id IN (SELECT hospitalization_id FROM no_bc_hosps)
                      AND LOWER(m.med_group) = 'vasoactives'
                      AND m.med_dose > 0
                      AND (a.location_category IS NULL OR LOWER(a.location_category) != 'procedural')
                    GROUP BY m.hospitalization_id
                ),

                -- IMV (first episode)
                imv AS (
                    SELECT
                        hospitalization_id,
                        MIN(recorded_dttm) AS imv_dttm
                    FROM respiratory
                    WHERE hospitalization_id IN (SELECT hospitalization_id FROM no_bc_hosps)
                      AND LOWER(device_category) = 'imv'
                    GROUP BY hospitalization_id
                ),

                -- Labs
                labs_agg AS (
                    SELECT
                        hospitalization_id,
                        MIN(CASE WHEN LOWER(lab_category) = 'lactate'
                                 AND COALESCE(lab_value_numeric, TRY_CAST(lab_value AS DOUBLE)) >= 2.0
                            THEN COALESCE(lab_result_dttm, lab_order_dttm) END) AS lactate_dttm,
                        MIN(CASE WHEN LOWER(lab_category) = 'platelet_count'
                                 AND COALESCE(lab_value_numeric, TRY_CAST(lab_value AS DOUBLE)) < 100
                            THEN COALESCE(lab_result_dttm, lab_order_dttm) END) AS thrombocytopenia_dttm,
                        MIN(CASE WHEN LOWER(lab_category) = 'bilirubin_total'
                                 AND COALESCE(lab_value_numeric, TRY_CAST(lab_value AS DOUBLE)) >= 2.0
                            THEN COALESCE(lab_result_dttm, lab_order_dttm) END) AS hyperbilirubinemia_dttm
                    FROM labs
                    WHERE hospitalization_id IN (SELECT hospitalization_id FROM no_bc_hosps)
                      AND LOWER(lab_category) IN ('lactate', 'platelet_count', 'bilirubin_total')
                    GROUP BY hospitalization_id
                ),

                -- ESRD (use the esrd_patients table that should still exist)
                esrd AS (
                    SELECT DISTINCT hospitalization_id, 1 AS has_esrd
                    FROM esrd_patients
                    WHERE hospitalization_id IN (SELECT hospitalization_id FROM no_bc_hosps)
                )

                SELECT
                    h.hospitalization_id,
                    v.vasopressor_dttm,
                    v.vasopressor_name,
                    i.imv_dttm,
                    l.lactate_dttm,
                    l.thrombocytopenia_dttm,
                    l.hyperbilirubinemia_dttm,
                    COALESCE(e.has_esrd, 0) AS has_esrd
                FROM no_bc_hosps h
                LEFT JOIN vaso v ON h.hospitalization_id = v.hospitalization_id
                LEFT JOIN imv i ON h.hospitalization_id = i.hospitalization_id
                LEFT JOIN labs_agg l ON h.hospitalization_id = l.hospitalization_id
                LEFT JOIN esrd e ON h.hospitalization_id = e.hospitalization_id
            """).df()


            # Create base dataframe for non-BC patients
            no_bc_df = no_bc_clinical.copy()
            no_bc_df['bc_id'] = pd.NA
            no_bc_df['episode_id'] = pd.NA
            no_bc_df['type'] = pd.NA
            no_bc_df['presumed_infection'] = 0
            no_bc_df['sepsis'] = 0
            no_bc_df['sepsis_wo_lactate'] = 0
            no_bc_df['no_sepsis_reason'] = 'no_blood_culture'

            # Add remaining columns as NA
            for col in ase_df.columns:
                if col not in no_bc_df.columns:
                    no_bc_df[col] = pd.NA

            ase_df = pd.concat([ase_df, no_bc_df], ignore_index=True)

        # Standardize NA values in no_sepsis_reason
        ase_df['no_sepsis_reason'] = ase_df['no_sepsis_reason'].replace({np.nan: pd.NA})

        # Select and order final columns
        final_columns = [
            "hospitalization_id",
            "bc_id",
            "episode_id",
            "type",
            "presumed_infection",
            "sepsis",
            "sepsis_wo_lactate",
            "no_sepsis_reason",
            "blood_culture_dttm",
            "total_qad",
            "qad_start_date",
            "qad_end_date",
            "first_qad_dttm",
            "presumed_infection_onset_dttm",
            "ase_onset_w_lactate_dttm",
            "ase_first_criteria_w_lactate",
            "ase_onset_wo_lactate_dttm",
            "ase_first_criteria_wo_lactate",
            "vasopressor_dttm",
            "vasopressor_name",
            "imv_dttm",
            "aki_dttm",
            "hyperbilirubinemia_dttm",
            "thrombocytopenia_dttm",
            "lactate_dttm",
            "has_esrd",
            "anchor_meds_in_window",
            "anchor_parenteral_meds_in_window",
            "run_meds",
            "final_qad_status"
        ]

        # Add missing columns with NA values if not present
        for col in final_columns:
            if col not in ase_df.columns:
                ase_df[col] = pd.NA

        # Select final columns
        ase_results = ase_df[final_columns].copy()

    finally:
        # Cleanups
        con.close()
        gc.collect()

    if verbose:
        print(f"\n{'='*60}")
        print(f"ASE COMPUTATION COMPLETE")
        print(f"Total ASE events: {len(ase_results[ase_results['sepsis']==1])}")
        print(f"Total hospitalizations processed: {len(ase_results['hospitalization_id'].unique())}")
        print(f"{'='*60}\n")

    return ase_results

# Alias
ase = compute_ase