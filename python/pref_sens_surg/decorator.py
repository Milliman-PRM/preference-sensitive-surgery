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
    
    ccs_columns = [c.ccs for c in dfs_refs['icd_procs'].select('ccs').distinct().collect()]

    claim_seqnum = dfs_input['outclaims'].select(
            'sequencenumber',
        )
    
    for ccs in sorted(ccs_columns):
        claim_seqnum = claim_seqnum.withColumn(
                ccs,
                spark_funcs.lit('N')
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
       
    final_ccs = inpatient_transfer_filter.union(
            outpatient_ip_filter
        ).withColumn(
            'ccs_flag',
            spark_funcs.lit('Y')
        )
    
    ccs_pivot = final_ccs.select(
            'sequencenumber',
            'ccs',
            'ccs_flag',
        ).groupBy(
            'sequencenumber',
        ).pivot(
            'ccs',
        ).agg(
            spark_funcs.first('ccs_flag')
        )
    
    for column in [c for c in ccs_pivot.columns if 'ccs' in c]:
        ccs_pivot = ccs_pivot.withColumnRenamed(
                column,
                column + '_calc'
        )
        
    ccs_final = claim_seqnum.join(
            ccs_pivot,
            on='sequencenumber',
            how='left_outer',
        ).select(
            'sequencenumber',
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
from prm.spark.io_txt import build_structtype_from_csv

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
        'outclaims': sparkapp.session.read.csv(
            r"C:\Users\umang.gupta\Desktop\preference-sensitive-surgery\python\tests\Test Cases.csv",
            schema=build_structtype_from_csv(Path(r"C:\Users\umang.gupta\Desktop\preference-sensitive-surgery\python\tests\Test Cases Schema.csv")),
            sep=",",
            header=True,
            mode="FAILFAST",
            )
    }

