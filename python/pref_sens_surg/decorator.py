"""
CODE OWNERS: Umang Gupta, Pierre Cornell

OBJECTIVE:
    Maintain the logic for calculating preference-sensitive surgery decorators

DEVELOPER NOTES:
    Will share some tooling with analytics-pipeline library
"""

import logging

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as spark_funcs
import pyspark.sql.types as spark_types

from prm.decorators.base_classes import ClaimDecorator

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def _collect_pss_eligible_ip_surg(
        outclaims: DataFrame,
        ref_table: DataFrame,
    ) -> DataFrame:
    """Flag potential inpatient preference sensitive surgeries"""

    outclaims_filter = outclaims.where(
            (spark_funcs.col('mr_line_case').startswith('I12')) &
            (spark_funcs.col('mr_allowed') > 0)
        )    

    icd_proc =  [
            spark_funcs.col(col_name)
            for col_name in outclaims_filter.columns
            if col_name.startswith('icdproc')
        ]
    
    max_claim_allowed = outclaims_filter.select(
            'claimid',
            'member_id',
            'mr_allowed',
        ).groupBy(
            'claimid',
            'member_id',
        ).agg(
            spark_funcs.max(spark_funcs.col('mr_allowed')).alias('max_allowed')
        )
    
    claim_elig_procs = outclaims_filter.join(
            max_claim_allowed,
            on=(outclaims_filter.claimid == max_claim_allowed.claimid) &
               (outclaims_filter.mr_allowed == max_claim_allowed.max_allowed),
            how='inner'
        ).select(
            'sequencenumber',
            outclaims_filter.member_id,
            'prm_fromdate',
            spark_funcs.array(icd_proc).alias('icd_proc'),
        )
            

    df_proc_pivot = claim_elig_procs.select(
            'sequencenumber',
            'member_id',
            'prm_fromdate',
            spark_funcs.explode('icd_proc').alias('icd_proc'),
        ).filter(
            spark_funcs.col('icd_proc').isNotNull()
        )
    
    proc_w_ccs = df_proc_pivot.join(
            ref_table,
            on=(ref_table.code == df_proc_pivot.icd_proc),
            how='inner',
        )
    
    return proc_w_ccs

def _collect_pss_eligible_op_surg(
        outclaims: DataFrame,
        ref_table: DataFrame,
    ) -> DataFrame:
    """Flag potential outpatient preference sensitive surgeries"""

    outclaims_filter = outclaims.where(
            (spark_funcs.col('mr_line_case').startswith('O12')) &
            (spark_funcs.col('mr_allowed') > 0)
        )    
    
    max_claim_allowed = outclaims_filter.select(
            'claimid',
            'member_id',
            'mr_allowed',
        ).groupBy(
            'claimid',
            'member_id',
        ).agg(
            spark_funcs.max(spark_funcs.col('mr_allowed')).alias('max_allowed')
        )
    
    claim_elig_hcpcs = outclaims_filter.join(
            max_claim_allowed,
            on=(outclaims_filter.claimid == max_claim_allowed.claimid) &
               (outclaims_filter.mr_allowed == max_claim_allowed.max_allowed),
            how='inner'
        )
    
    hcpcs_w_ccs = claim_elig_hcpcs.join(
            ref_table,
            on=(claim_elig_hcpcs.hcpcs == ref_table.code),
            how='inner',
        )
    
    return hcpcs_w_ccs

def _flag_elig_drgs(
        outclaims: DataFrame,
        inpatient_pss: DataFrame,
        ref_table: DataFrame,
    ) -> DataFrame:
    
    inpatient_pss_drg = inpatient_pss.join(
                outclaims,
                on='sequencenumber',
                how='inner',
            ).select(
                inpatient_pss.sequencenumber,
                'icd_proc',
                'ccs',
                'drg',
            ).join(
                ref_table,
                on='ccs',
                how='inner',
            ).where(
                spark_funcs.col('drg') == spark_funcs.col('code')
            )
    
    return inpatient_pss_drg
    
        
def calculate_pss_decorator(
        dfs_input: "typing.Mapping[str, DataFrame]",
        dfs_refs: "typing.Mapping[str, DataFrame]",
        **kwargs
    ) -> DataFrame:

    """Flag eligible inpatient and outpatient flags"""
    LOGGER.info('Calculating preference-senstive surgery decorators')

    inpatient_surgery = _collect_pss_eligible_ip_surg(
            outclaims=dfs_input['outclaims'],
            ref_table=dfs_refs['icd_procs'],         
        )
        
    inpatient_drg_filter = _flag_elig_drgs(
                outclaims=dfs_input['outclaims'],
                inpatient_pss=inpatient_surgery,
                ref_table=dfs_refs['drg'],
            )
    
    outpatient_surgery = _collect_pss_eligible_op_surg(
            outclaims=dfs_input['outclaims'],
            ref_table=dfs_refs['hcpcs'],
        )
    
    
    
    return inpatient_drg_filter
        
class PSSDecorator(ClaimDecorator):
    """Calculate the preference-sensitive surgery decorators"""
    @staticmethod
    def validate_decor_column_name(name: str) -> bool:
        """Defines what naming convention the decorator columns should follow"""
        return name.startswith("pss_")

    def _calc_decorator(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            dfs_refs: "typing.Mapping[str, DataFrame]",
            **kwargs
        ) -> DataFrame:
        
        return calculate_pss_decorator(
            dfs_input,
            dfs_refs,
            **kwargs,
        )


""" Run these stuff prior to testing out everything """
""" Placeholder for before we integrate SDW """
from pathlib import Path
from prm.spark.app import SparkApp
import prm_ny_data_share.meta.project

META_SHARED = prm_ny_data_share.meta.project.gather_metadata()
PATH_LOCAL_REFS = Path(r'C:\Users\umang.gupta\Desktop\preference-sensitive-surgery\ref_tables') 

sparkapp = SparkApp(META_SHARED['pipeline_signature'])

paths_local_refs = {
    PATH_LOCAL_REFS / 'ref_CCS113_turp.csv',
    PATH_LOCAL_REFS / 'ref_CCS124_hysterectomy.csv',
    PATH_LOCAL_REFS / 'ref_CCS149_arthroscopy.csv',
    PATH_LOCAL_REFS / 'ref_CCS152_arthroplasty_knee.csv',
    PATH_LOCAL_REFS / 'ref_CCS153_hip_replacement.csv',
    PATH_LOCAL_REFS / 'ref_CCS154_arthroplasty_other.csv',
    PATH_LOCAL_REFS / 'ref_CCS158_spinal_fusion.csv',
    PATH_LOCAL_REFS / 'ref_CCS244_gastric_bypass.csv',
    PATH_LOCAL_REFS / 'ref_CCS3_laminectomy.csv',
    PATH_LOCAL_REFS / 'ref_CCS44_cabg.csv',
    PATH_LOCAL_REFS / 'ref_CCS45_ptca.csv',
    PATH_LOCAL_REFS / 'ref_CCS48_pacemaker_defibrillator.csv',
    PATH_LOCAL_REFS / 'ref_CCS51_59_head_neck_vessel.csv',
    PATH_LOCAL_REFS / 'ref_CCS55_61_peripheral_vessel.csv',
    PATH_LOCAL_REFS / 'ref_CCS84_cholecystectomy.csv',
    }

dfs_refs_raw = {
        path.stem: sparkapp.session.read.csv(
            str(path),
            header=True,
            )
        for path in paths_local_refs
    }

refs_stack = dfs_refs_raw['ref_CCS149_arthroscopy'].union(
                dfs_refs_raw['ref_CCS113_turp']
            ).union(
                dfs_refs_raw['ref_CCS45_ptca']
            ).union(
                dfs_refs_raw['ref_CCS158_spinal_fusion']
            ).union(
                dfs_refs_raw['ref_CCS44_cabg']
            ).union(
                dfs_refs_raw['ref_CCS153_hip_replacement']
            ).union(
                dfs_refs_raw['ref_CCS154_arthroplasty_other']
            ).union(
                dfs_refs_raw['ref_CCS84_cholecystectomy']
            ).union(
                dfs_refs_raw['ref_CCS152_arthroplasty_knee']
            ).union(
                dfs_refs_raw['ref_CCS3_laminectomy']
            ).union(
                dfs_refs_raw['ref_CCS244_gastric_bypass']
            ).union(
                dfs_refs_raw['ref_CCS51_59_head_neck_vessel']
            ).union(
                dfs_refs_raw['ref_CCS124_hysterectomy']
            ).union(
                dfs_refs_raw['ref_CCS48_pacemaker_defibrillator']
            ).union(
                dfs_refs_raw['ref_CCS55_61_peripheral_vessel']
            ).coalesce(15)

dfs_refs = {
        'icd_procs': refs_stack.where(spark_funcs.col('code_type') == 'ICD10_PROC'),
        'hcpcs': refs_stack.where(spark_funcs.col('code_type') == 'CPT_HCPCS'),
        'drg': refs_stack.where(spark_funcs.col('code_type') == 'MSDRG'),
    }

dfs_input = {
    'outclaims': sparkapp.load_df(
        META_SHARED[73, 'out'] / 'outclaims_prm.parquet'
        ).select(
            '*',
            spark_funcs.col('prm_prv_id_ccn').alias('prv_id_ccn')
            ),
    }

