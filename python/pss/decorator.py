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

    max_claim_allowed = outclaims_filter.select(
            'claim_number',
            'member_id',
            'mr_allowed',
        ).groupBy(
            'claim_number',
            'member_id',
        ).agg(
            spark_funcs.max(spark_funcs.col('mr_allowed').alias('max_allowed'))
        )
    
    claim_elig_procs = outclaims_filter.join(
            max_claim_allowed,
            on=(outclaims_filter.claimid == max_claim_allowed.claimid) &
               (outclaims_filter.mr_allowed == max_claim_allowed.max_allowed),
            how='inner'
        )
    
    icd_proc =  [
            spark_funcs.col(col_name)
            for col_name in claim_elig_procs.columns
            if col_name.startswith('icdproc')
        ]

    df_proc_pivot = claim_elig_procs.select(
            'sequencenumber',
            spark_funcs.explode(spark_funcs.array(icd_proc)).alias('icd_proc'),
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
    """Flag potential inpatient preference sensitive surgeries"""

    outclaims_filter = outclaims.where(
            (spark_funcs.col('mr_line_case').startswith('O12')) &
            (spark_funcs.col('mr_allowed') > 0)
        )    
    
    max_claim_allowed = outclaims_filter.select(
            'claim_number',
            'member_id',
            'mr_allowed',
        ).groupBy(
            'claim_number',
            'member_id',
        ).agg(
            spark_funcs.max(spark_funcs.col('mr_allowed').alias('max_allowed'))
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
        
def calculate_pss_decorator(
        dfs_input: "typing.Mapping[str, DataFrame]",
        dfs_refs: "typing.Mapping[str, DataFrame]",
        **kwargs
    ) -> DataFrame:

    """Flag eligible inpatient and outpatient flags"""
    LOGGER.info('Calculating preference-senstive surgery decorators')

    inpatient_surgery = _collect_pss_eligible_ip_surg(
            outclaims=dfs_input['outclaims'],
            ref_table=dfs_refs['icd_codes'],         
        )
        
    outpatient_surgery = _collect_pss_eligible_op_surg(
            outclaims=dfs_input['outclaims'],
            ref_table=dfs_refs['hcpcs'],
        )
        
        
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
