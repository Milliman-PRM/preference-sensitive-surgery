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

er_hcpcs = ["99281","99282","99283","99284","99285","99286","99287","99288","G0380","G0381","G0382","G0383","G0384"]
er_rev = ["0450","0451","0452","0456","0459","0981"]
ip_only_ccs = ["ccs152", "ccs153", "ccs158"]

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
               (outclaims_filter.mr_allowed == max_claim_allowed.max_allowed) &
               (outclaims_filter.member_id == max_claim_allowed.member_id),
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
        ).select(
            'member_id',
            'sequencenumber',
            'prm_fromdate',
            'ccs',
        ).distinct()
    
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
               (outclaims_filter.mr_allowed == max_claim_allowed.max_allowed) &
               (outclaims_filter.member_id == max_claim_allowed.member_id),
            how='inner',
        ).select(
            outclaims_filter.member_id,
            'sequencenumber',
            'prm_fromdate',
            'hcpcs',
        )
    
    hcpcs_w_ccs = claim_elig_hcpcs.join(
            ref_table,
            on=(claim_elig_hcpcs.hcpcs == ref_table.code),
            how='inner',
        ).select(
            'member_id',
            'sequencenumber',
            'prm_fromdate',
            'ccs',
        ).distinct()
    
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
            inpatient_pss.sequencenumber.alias('sequencenumber'),
            inpatient_pss.member_id.alias('member_id'),
            inpatient_pss.prm_fromdate.alias('prm_fromdate'),
            'ccs',
            'drg',
        ).join(
            ref_table,
            on='ccs',
            how='inner',
        ).where(
            spark_funcs.col('drg') == spark_funcs.col('code')
        ).select(
            'member_id',
            'sequencenumber',
            'prm_fromdate',
            'ccs',
        ).distinct()
    
    return inpatient_pss_drg
    
        
def _flag_er_directed(
        outclaims: DataFrame,
        pss_claims: DataFrame,
    ) -> DataFrame:

    ed_claims = outclaims.select(
            'member_id',
            spark_funcs.col('prm_fromdate').alias('ed_date'),
            spark_funcs.when(
               spark_funcs.col('hcpcs').isin(er_hcpcs),
               spark_funcs.lit('Y'),
            ).when(
                spark_funcs.col('revcode').isin(er_rev),
                spark_funcs.lit('Y'),
            ).otherwise(
                spark_funcs.lit('N')
            ).alias('er_flag'),
        ).where(
            spark_funcs.col('er_flag') == 'Y'
        ).distinct()


    pss_claims_no_er = pss_claims.join(
            ed_claims,
            on='member_id',
            how='left_outer',
        ).where(
            spark_funcs.col('ed_date').isNull() |
            ~spark_funcs.col('ed_date').between(
                    spark_funcs.date_sub(
                        spark_funcs.col('prm_fromdate'),
                        1,
                    ),
                    spark_funcs.col('prm_fromdate'),
            )
        ).select(
            'member_id',
            'sequencenumber',
            'prm_fromdate',
            'ccs',
        ).distinct()
        
    return pss_claims_no_er

def _flag_acute_transfer(
        outclaims: DataFrame,
        pss_claims: DataFrame,
    ) -> DataFrame:

    acute_transfers = outclaims.select(
            'member_id',
            spark_funcs.col('prm_todate').alias('transfer_date')
        ).where(
            spark_funcs.col('prm_acute_transfer_to_acute_yn') == 'Y'
        ).distinct()
    
    pss_claims_no_transfer = pss_claims.join(
            acute_transfers,
            on='member_id',
            how='left_outer',
        ).where(
            spark_funcs.col('transfer_date').isNull() |
            ~spark_funcs.col('transfer_date').between(
                    spark_funcs.date_sub(
                        spark_funcs.col('prm_fromdate'),
                        1,
                    ),
                    spark_funcs.col('prm_fromdate'),
            )
        ).select(
            'member_id',
            'sequencenumber',
            'prm_fromdate',
            'ccs',
        ).distinct()
                    
    return pss_claims_no_transfer
    
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
        
    inpatient_er_filter = _flag_er_directed(
            outclaims=dfs_input['outclaims'],
            pss_claims=inpatient_drg_filter,
        )
    
    inpatient_transfer_filter = _flag_acute_transfer(
            outclaims=dfs_input['outclaims'],
            pss_claims=inpatient_er_filter
        )
    
    outpatient_surgery = _collect_pss_eligible_op_surg(
            outclaims=dfs_input['outclaims'],
            ref_table=dfs_refs['hcpcs'],
        )
    
    outpatient_er_filter = _flag_er_directed(
            outclaims=dfs_input['outclaims'],
            pss_claims=outpatient_surgery,
        )
    
    outpatient_ip_filter = outpatient_er_filter.where(
            ~spark_funcs.col('ccs').isin(ip_only_ccs)
        )
    
    
    op_ip_ccs = inpatient_transfer_filter.union(
            outpatient_ip_filter
        ).withColumnRenamed(
            'ccs',
            'ccs_calc',
        ).withColumn(
            'ccs_flag_calc',
            spark_funcs.lit('Y')
        )
    
    unique_ccs = dfs_refs['icd_procs'].select(
            'ccs',
        ).orderBy(
            'ccs',
        ).distinct()
    
    ccs_eligible_w_flags = dfs_input['outclaims'].select(
            'sequencenumber',
            'mr_line_case',
        ).withColumn(
            'ccs_eligible_yn',
            spark_funcs.when(
                spark_funcs.col('mr_line_case').isin(
                    ['O12', 'I12'],
                ),
                spark_funcs.lit('Y')
            ).otherwise(
                spark_funcs.lit('N')
            ),
        ).crossJoin(
            unique_ccs
        ).withColumn(
            'ccs_flag',
            spark_funcs.lit('N')
        )
         
    ccs_calc_final = ccs_eligible_w_flags.join(
            op_ip_ccs,
            on=(ccs_eligible_w_flags.sequencenumber == op_ip_ccs.sequencenumber) &
               (ccs_eligible_w_flags.ccs == op_ip_ccs.ccs_calc),
            how='left_outer',
        ).select(
            ccs_eligible_w_flags.sequencenumber,
            'ccs_eligible_yn',
            'ccs',
            spark_funcs.coalesce('ccs_flag_calc', 'ccs_flag').alias('ccs_flag')
        )
    
    ccs_calc_pivot = ccs_calc_final.groupBy(
            'sequencenumber',
            'ccs_eligible_yn',
        ).pivot(
            'ccs',
        ).agg(
            spark_funcs.first('ccs_flag')
        )
    
    return ccs_calc_pivot
        
class PSSDecorator(ClaimDecorator):
    """Calculate the preference-sensitive surgery decorators"""
    @staticmethod
    def validate_decor_column_name(name: str) -> bool:
        """Defines what naming convention the decorator columns should follow"""
        return name.startswith("ccs_")

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