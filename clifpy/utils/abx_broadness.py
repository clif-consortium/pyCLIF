"""
Antibiotic Broadness Scores

This module provides functionality to calculate the Narrow Antibiotic Therapy
(NAT) score to quantify antibiotic broadness. More metrics will be added in
the future.
"""

import pandas as pd
import duckdb
from typing import List

# ——— NAT Score —————————————————————————————————————————————————————————————

### Define antibiotic categories
# Combo NAT Score: 3
COMBO3: List[str] = [
    'ceftazidime_avibactam',
    'ceftolozane_tazobactam',
    'colistin',
    'colistin_polymyxin',
    'cefiderocol',
    'meropenem_vaborbactam'
]

# Combo NAT Score: 2
COMBO2: List[str] = ['meropenem']

# Combo NAT Score: 1
COMBO1: List[str] = [
    'linezolid',
    'sulfamethoxazole',
    'ertapenem',
    'aztreonam',
    'cefepime',
    'ceftazidime',
    'piperacillin_tazobactam',
    'vancomycin',
    'amikacin',
    'gentamicin',
    'metronidazole',
    'neomycin', 
    'paromomycin', 
    'plazomicin', 
    'streptomycin', 
    'tobramycin'
]

# Combo NAT score: 0
COMBO0: List[str] = [
    'amoxicillin',
    'amoxicillin_clavulanate',
    'ampicillin_sulbactam',
    'azithromycin',
    'cefazolin',
    'cefotaxime',
    'cefuroxime',
    'cefpodoxime',
    'ceftriaxone',
    'clarithromycin',
    'clindamycin',
    'doxycycline',
    'levofloxacin',
    'ciprofloxacin',
    'moxifloxacin'
]

# Mono NAT Score: 3
MONO3: List[str] = [
    'ceftazidime_avibactam',
    'ceftolozane_tazobactam',
    'colistin',
    'colistin_polymyxin',
    'cefiderocol',
    'meropenem_vaborbactam'
]

# Mono NAT Score: 2
MONO2: List[str] = ['meropenem']

# Mono NAT Score: 1
MONO1: List[str] = [
    'ertapenem',
    'aztreonam',
    'cefepime',
    'ceftazidime',
    'piperacillin_tazobactam',
    'vancomycin',
]

# Mono NAT Score: -1
MONONEG1: List[str] = [
    'amoxicillin',
    'amoxicillin_clavulanate',
    'ampicillin_sulbactam',
    'azithromycin',
    'cefazolin',
    'cefotaxime',
    'cefuroxime',
    'cefpodoxime',
    'ceftriaxone',
    'clarithromycin',
    'clindamycin',
    'doxycycline',
    'levofloxacin',
    'ciprofloxacin',
    'moxifloxacin',
    'linezolid',
    'sulfamethoxazole'
]

# Mono NAT Score: -2
MONONEG2: List[str] = [
    'amikacin',
    'gentamicin',
    'metronidazole',
    'neomycin',
    'paromomycin', 
    'plazomicin', 
    'streptomycin',
    'tobramycin'
]

def compute_nat(
    med_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute NAT score. 

    Parameters:
        med_df: DataFrame of administered medications. Compatible with data
            loaded from 'clif_medication_admin_intermittent.parquet'. Should
            contain the following columns:
                - `hospitalization_id`: str
                - `med_category`: str
                - `mar_action_group`: str
                - `admin_dttm`: datetime64[ns, UTC]

    Returns:
        nat_df: DataFrame with computed NAT score per date. Contains the
            following columns:
                - `hospitalization_id`: str
                - `admin_dt`: datetime64[ns] 
                - `nat_score`: float64
        
    """
    # Filter for antibiotics
    med_list = COMBO3 + COMBO2 + COMBO1 + COMBO0 + MONO3 + MONO2 + MONO1 \
        + MONONEG1 + MONONEG2
    
    med_df2 = med_df[['hospitalization_id', 'med_category', 'mar_action_group',
        'admin_dttm']]
    
    abx_df = med_df2[(
        (med_df2["med_category"].isin(med_list))
        & (med_df2["mar_action_group"] == "administered")
    )].drop(columns=["mar_action_group"])

    # Assign combo and mono score
    query = f"""
        FROM abx_df df
        SELECT df.*
        , nat_combo: CASE 
            WHEN med_category = ANY($combo_3) THEN 3
            WHEN med_category = ANY($combo_2) THEN 2
            WHEN med_category = ANY($combo_1) THEN 1
            WHEN med_category = ANY($combo_0) THEN 0
        END
        , nat_mono: CASE
            WHEN med_category = ANY($mono_3) THEN 3
            WHEN med_category = ANY($mono_2) THEN 2
            WHEN med_category = ANY($mono_1) THEN 1
            WHEN med_category = ANY($mono_neg1) THEN -1
            WHEN med_category = ANY($mono_neg2) THEN -2
        END
    """
    params = {
        "combo_3": COMBO3,
        "combo_2": COMBO2,
        "combo_1": COMBO1,
        "combo_0": COMBO0,
        "mono_3": MONO3,
        "mono_2": MONO2,
        "mono_1": MONO1,
        "mono_neg1": MONONEG1,
        "mono_neg2": MONONEG2
    }
    abx_df2 = duckdb.sql(query, params=params).df()

    # Select relevant columns and de-dup
    abx_df2['admin_dt'] = abx_df2['admin_dttm'].dt.tz_convert('UTC')\
        .dt.tz_localize(None).dt.date
    abx_df3 = abx_df2[[
        'hospitalization_id', 'med_category', 'nat_combo', 'nat_mono', 'admin_dt'
    ]].drop_duplicates()

    # Create helper table to determine combo vs. mono NAT calculations
    query = f"""
        WITH temp_med_rank AS (
            FROM abx_df3 df
            SELECT hospitalization_id
            , admin_dt
            , med_rank: Row_number() OVER (
                PARTITION BY hospitalization_id, admin_dt
                ORDER BY med_category
            )
        )
        FROM temp_med_rank
        SELECT hospitalization_id
        , admin_dt
        , max_med_rank: MAX(med_rank)
        GROUP BY hospitalization_id, admin_dt
    """
    helper_df = duckdb.sql(query).df()

    # Calculate NAT score
    query = f"""
        FROM abx_df3 df
        LEFT JOIN helper_df h
            ON df.hospitalization_id = h.hospitalization_id
            AND df.admin_dt = h.admin_dt
        SELECT df.hospitalization_id
        , df.admin_dt
        , nat_score: CASE
            WHEN h.max_med_rank > 1 THEN sum(nat_combo)
            WHEN h.max_med_rank == 1 THEN sum(nat_mono)
        END
        GROUP BY df.hospitalization_id, df.admin_dt, h.max_med_rank
    """
    nat_df = duckdb.sql(query).df()
    nat_df['admin_dt'] = nat_df['admin_dt'].astype('datetime64[ns]')

    return nat_df



