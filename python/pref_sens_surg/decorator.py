"""
CODE OWNERS: Umang Gupta, Pierre Cornell

OBJECTIVE:
    Maintain the logic for calculating preference-sensitive surgery decorators

DEVELOPER NOTES:
    Will share some tooling with analytics-pipeline library
"""
# pylint: disable=no-member
import logging
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as spark_funcs


from prm.decorators.base_classes import ClaimDecorator

LOGGER = logging.getLogger(__name__)

ER_HCPCS = ["99281", "99282", "99283", "99284", "99285", "99286", "99287", "99288", "G0380",
            "G0381", "G0382", "G0383", "G0384"]
ER_REV = ["0450", "0451", "0452", "0456", "0459", "0981"]
IP_ONLY_CCS = ["ccs152", "ccs153", "ccs158"]

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def _collect_pss_eligible_ip_surg(
        outclaims: DataFrame,
        ref_table: DataFrame,
    ) -> DataFrame:
    """Flag potential inpatient preference sensitive surgeries"""

    outclaims_filter = outclaims.where(
        spark_funcs.col('mr_line_case').startswith('I12')
    )

    icd_proc = [
        spark_funcs.col(col_name)
        for col_name in outclaims_filter.columns
        if col_name.startswith('icdproc')
    ]

    df_proc_pivot = outclaims_filter.select(
        'caseadmitid',
        'sequencenumber',
        'member_id',
        'prm_fromdate',
        spark_funcs.array(icd_proc).alias('icd_proc')  
    ).select(
        'caseadmitid',
        'sequencenumber',
        'member_id',
        'prm_fromdate',
        spark_funcs.posexplode('icd_proc').alias('icd_position', 'icd_proc'),
    ).filter(
        spark_funcs.col('icd_proc').isNotNull()
    )

    proc_w_ccs = df_proc_pivot.join(
        ref_table,
        on=(ref_table.code == df_proc_pivot.icd_proc),
        how='inner',
    ).select(
        'caseadmitid',
        'sequencenumber',
        'member_id',
        'prm_fromdate',
        spark_funcs.col('icd_position').alias('position'),
        'ccs',
    )

    return proc_w_ccs

def _collect_pss_eligible_op_surg(
        outclaims: DataFrame,
        ref_table: DataFrame,
    ) -> DataFrame:
    """Flag potential outpatient preference sensitive surgeries"""

    outclaims_filter = outclaims.where(
        spark_funcs.col('mr_line_case').startswith('O12')
    )

    hcpcs_w_ccs = outclaims_filter.join(
        ref_table,
        on=(outclaims_filter.hcpcs == ref_table.code),
        how='inner',
    ).select(
        'member_id',
        'caseadmitid',
        'sequencenumber',
        'prm_fromdate',
        'ccs',
        spark_funcs.lit(0).alias('position'),
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
        inpatient_pss.caseadmitid.alias('caseadmitid'),
        inpatient_pss.sequencenumber.alias('sequencenumber'),
        inpatient_pss.member_id.alias('member_id'),
        inpatient_pss.prm_fromdate.alias('prm_fromdate'),
        'ccs',
        'position',
        'drg',
    ).join(
        ref_table,
        on='ccs',
        how='inner',
    ).where(
        spark_funcs.col('drg') == spark_funcs.col('code')
    ).select(
        'member_id',
        'caseadmitid',
        'sequencenumber',
        'prm_fromdate',
        'ccs',
        'position',
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
            spark_funcs.col('hcpcs').isin(ER_HCPCS),
            spark_funcs.lit('Y'),
        ).when(
            spark_funcs.col('revcode').isin(ER_REV),
            spark_funcs.lit('Y'),
        ).otherwise(
            spark_funcs.lit('N')
        ).alias('er_flag'),
    ).where(
        spark_funcs.col('er_flag') == 'Y'
    ).distinct()


    pss_claims_w_er = pss_claims.join(
        ed_claims,
        on='member_id',
        how='left_outer',
    ).where(
        spark_funcs.col('ed_date').between(
            spark_funcs.date_sub(
                spark_funcs.col('prm_fromdate'),
                1,
            ),
            spark_funcs.col('prm_fromdate'),
        )
    ).select(
        'member_id',
        'caseadmitid',
        'sequencenumber',
        'prm_fromdate',
        'ccs',
        'position',
        spark_funcs.lit('Y').alias('er_directed'),
    ).distinct()

    pss_claims_er_directed = pss_claims.join(
        pss_claims_w_er,
        on=['sequencenumber', 'ccs', 'position'],
        how='left_outer',
    ).select(
        pss_claims.member_id,
        pss_claims.caseadmitid, 
        pss_claims.sequencenumber,
        pss_claims.prm_fromdate,
        pss_claims.ccs,
        pss_claims.position,
        spark_funcs.when(
            spark_funcs.col('er_directed').isNull(),
            spark_funcs.lit('N'),
        ).otherwise(
            spark_funcs.lit('Y'),
        ).alias('er_directed'),
    )

    return pss_claims_er_directed

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

    pss_claims_w_transfer = pss_claims.join(
        acute_transfers,
        on='member_id',
        how='left_outer',
    ).where(
        spark_funcs.col('transfer_date').between(
            spark_funcs.date_sub(
                spark_funcs.col('prm_fromdate'),
                1,
            ),
            spark_funcs.col('prm_fromdate'),
        )
    ).select(
        'member_id',
        'caseadmitid',
        'sequencenumber',
        'prm_fromdate',
        'ccs',
        'position',
        spark_funcs.lit('Y').alias('transfer_yn')
    ).distinct()

    pss_claims_transfer = pss_claims.join(
        pss_claims_w_transfer,
        on=['sequencenumber', 'ccs', 'position'],
        how='left_outer',
    ).select(
        pss_claims.member_id,
        pss_claims.caseadmitid,
        pss_claims.sequencenumber,
        pss_claims.prm_fromdate,
        pss_claims.ccs,
        pss_claims.position,
        spark_funcs.when(
            spark_funcs.col('transfer_yn').isNull(),
            spark_funcs.lit('N'),
        ).otherwise(
            spark_funcs.lit('Y'),
        ).alias('transfer_yn')
    )

    return pss_claims_transfer

def _ip_dupe_filter(
        outclaims: "DataFrame",
        ip_pss: "DataFrame"
    ) -> DataFrame:
    
    caseadmit_window = Window().partitionBy(
        'caseadmitid',
    ).orderBy(
        'position',
        'prm_fromdate',
    )
    
    ip_pss_ranked = ip_pss.select(
        '*',
        spark_funcs.row_number().over(caseadmit_window).alias('order'),
    ).where(
        spark_funcs.col('order') == 1
    )
    
    ip_positive_allowed = ip_pss_ranked.join(
        outclaims,
        on='sequencenumber',
        how='inner',
    ).where(
        spark_funcs.col('mr_allowed') > 0
    ).select(
        ip_pss_ranked.member_id,
        ip_pss_ranked.caseadmitid,
        ip_pss_ranked.sequencenumber,
        ip_pss_ranked.prm_fromdate,
        ip_pss_ranked.ccs,
        ip_pss_ranked.position,
        'ccs_preventable_yn',
    )

    return ip_positive_allowed

def _op_dupe_filter(
        outclaims: "DataFrame",
        op_pss: "DataFrame"
    ) -> DataFrame:

    op_pss_w_paiddate = op_pss.join(
        outclaims,
        on='sequencenumber',
        how='inner'
    ).select(
        op_pss.member_id,
        op_pss.caseadmitid,
        op_pss.sequencenumber,
        op_pss.prm_fromdate,
        op_pss.ccs,
        op_pss.position,
        op_pss.ccs_preventable_yn,
        'paiddate',
        'mr_allowed',
    )
    
    pss_latest_paid = op_pss_w_paiddate.groupBy(
        'member_id',
        'prm_fromdate',
        'ccs',
    ).agg(
        spark_funcs.max(spark_funcs.col('paiddate')).alias('max_paiddate')
    )
    
    pss_latest_paid_flagged = op_pss_w_paiddate.join(
        pss_latest_paid,
        on=(op_pss_w_paiddate.member_id == pss_latest_paid.member_id)
        & (op_pss_w_paiddate.prm_fromdate == pss_latest_paid.prm_fromdate)
        & (op_pss_w_paiddate.ccs == pss_latest_paid.ccs)
        & (op_pss_w_paiddate.paiddate == pss_latest_paid.max_paiddate),
        how='left_outer',
    ).select(
        op_pss_w_paiddate.member_id,
        op_pss_w_paiddate.caseadmitid,
        op_pss_w_paiddate.sequencenumber,
        op_pss_w_paiddate.prm_fromdate,
        op_pss_w_paiddate.ccs,
        op_pss_w_paiddate.position,
        op_pss_w_paiddate.ccs_preventable_yn,
        op_pss_w_paiddate.paiddate,
        op_pss_w_paiddate.mr_allowed,
        'max_paiddate',
    )

    op_pss_filtered = pss_latest_paid_flagged.where(
        (~spark_funcs.col('max_paiddate').isNull())
        & (spark_funcs.col('mr_allowed') > 0)
    ).select(
        'member_id',
        'caseadmitid',
        'sequencenumber',
        'prm_fromdate',
        'ccs',
        'position',
        'ccs_preventable_yn',
    )
    
    return op_pss_filtered   

def _calc_ip_pss(
        dfs_input: "typing.Mapping[str, DataFrame]",
        dfs_refs: "typing.Mapping[str, DataFrame]"
    ) -> DataFrame:
    
    inpatient_surgery = _collect_pss_eligible_ip_surg(
        outclaims=dfs_input['outclaims'],
        ref_table=dfs_refs['icd_procs'],
    )

    inpatient_drg_filter = _flag_elig_drgs(
        outclaims=dfs_input['outclaims'],
        inpatient_pss=inpatient_surgery,
        ref_table=dfs_refs['drg'],
    )

    inpatient_transfer_directed = _flag_acute_transfer(
        outclaims=dfs_input['outclaims'],
        pss_claims=inpatient_drg_filter
    )

    inpatient_er_directed = _flag_er_directed(
        outclaims=dfs_input['outclaims'],
        pss_claims=inpatient_drg_filter,
    )

    inpatient_surgery_flagged = inpatient_transfer_directed.join(
        inpatient_er_directed,
        on=['sequencenumber', 'ccs', 'position'],
        how='inner',
    ).withColumn(
        'ccs_preventable_yn',
        spark_funcs.when(
            spark_funcs.col('transfer_yn') == 'Y',
            spark_funcs.lit('N'),
        ).when(
            spark_funcs.col('er_directed') == 'Y',
            spark_funcs.lit('N'),
        ).otherwise(
            spark_funcs.lit('Y')
        )
    ).select(
        inpatient_transfer_directed.member_id,
        inpatient_transfer_directed.caseadmitid,
        inpatient_transfer_directed.sequencenumber,
        inpatient_transfer_directed.prm_fromdate,
        inpatient_transfer_directed.ccs,
        inpatient_transfer_directed.position,
        'ccs_preventable_yn',
    )

    ip_pss_final = _ip_dupe_filter(
        outclaims=dfs_input['outclaims'],
        ip_pss=inpatient_surgery_flagged,
    )
    
    return ip_pss_final

def _calc_op_pss(
        dfs_input: "typing.Mapping[str, DataFrame]",
        dfs_refs: "typing.Mapping[str, DataFrame]"
    ) -> DataFrame:

    outpatient_surgery = _collect_pss_eligible_op_surg(
        outclaims=dfs_input['outclaims'],
        ref_table=dfs_refs['hcpcs'],
    )

    outpatient_er_directed = _flag_er_directed(
        outclaims=dfs_input['outclaims'],
        pss_claims=outpatient_surgery,
    )

    outpatient_ip_filter = outpatient_er_directed.where(
        ~spark_funcs.col('ccs').isin(IP_ONLY_CCS)
    ).withColumn(
        'ccs_preventable_yn',
        spark_funcs.when(
            spark_funcs.col('er_directed') == 'Y',
            spark_funcs.lit('N'),
        ).otherwise(
            spark_funcs.lit('Y'),
        )
    ).select(
        'member_id',
        'caseadmitid',
        'sequencenumber',
        'prm_fromdate',
        'ccs',
        'position',
        'ccs_preventable_yn',
    )
    
    op_pss_final = _op_dupe_filter(
        outclaims=dfs_input['outclaims'],
        op_pss=outpatient_ip_filter,
    )

    return op_pss_final
    
def calculate_pss_decorator(
        dfs_input: "typing.Mapping[str, DataFrame]",
        dfs_refs: "typing.Mapping[str, DataFrame]",
        **kwargs
    ) -> DataFrame:

    """Flag eligible inpatient and outpatient flags"""
    LOGGER.info('Calculating preference-senstive surgery decorators')

    inpatient_pss = _calc_ip_pss(
        dfs_input=dfs_input,
        dfs_refs=dfs_refs,
    )

    outpatient_pss = _calc_op_pss(
        dfs_input=dfs_input,
        dfs_refs=dfs_refs,
    )
    
    op_ip_pss = inpatient_pss.union(
        outpatient_pss
    )

    ccs_eligible_w_flags = dfs_input['outclaims'].select(
        'sequencenumber',
        'mr_line_case',
    ).withColumn(
        'ccs_eligible_yn',
        spark_funcs.when(
            spark_funcs.col('mr_line_case').startswith('I12'),
            spark_funcs.lit('Y')
        ).when(
            spark_funcs.col('mr_line_case').startswith('O12'),
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        ),
    )

    ccs_calc = ccs_eligible_w_flags.join(
        op_ip_pss,
        on='sequencenumber',
        how='left_outer',
    ).select(
        ccs_eligible_w_flags.sequencenumber,
        spark_funcs.col('ccs_eligible_yn').alias('psp_eligible_yn'),
        spark_funcs.col('ccs').alias('psp_category'),
        spark_funcs.col('ccs_preventable_yn').alias('psp_preventable_yn'),
    )

    return ccs_calc

class PSSDecorator(ClaimDecorator):
    """Calculate the preference-sensitive surgery decorators"""
    @staticmethod
    def validate_decor_column_name(name: str) -> bool:
        """Defines what naming convention the decorator columns should follow"""
        return name.startswith("psp_")

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
