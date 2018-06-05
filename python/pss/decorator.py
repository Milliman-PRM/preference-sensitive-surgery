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
            (spark_funcs.col('mr_allowed') > 0) &
            (spark_funcs.col('admitsource') != '4')
        )    
    
    icd_proc =  [
            spark_funcs.col(col_name)
            for col_name in outclaims_filter.columns
            if col_name.startswith('icdproc')
        ]

    df_proc_pivot = outclaims_filter.select(
            'claimid',
            'member_id',
            'prm_fromdate',
            'revcode',
            'hcpcs',
            'mr_allowed',
            spark_funcs.explode(spark_funcs.array(icd_proc)).alias('icd_proc'),
        ).filter(
            spark_funcs.col('icd_proc').isNotNull()
        ).groupBy(
            'claimid',
            'member_id',
            'prm_fromdate',
            'revcode',
            'hcpcs',
            'icd_proc'
        ).agg(
            spark_funcs.max(spark_funcs.col('mr_allowed'))
        )
    
    proc_w_ccs = df_proc_pivot.join(
                ref_table,
                on=(ref_table.code == df_proc_pivot.icd_proc),
                how='left_outer',
            )
    
    return proc_w_ccs.where(spark_funcs.col('ccs').isNotNull())
    
    
def calculate_pss_decorator(
        dfs_input: "typing.Mapping[str, DataFrame]",
        *
    ) -> DataFrame:

    """Flag eligible inpatient and outpatient flags"""
    LOGGER.info('Calculating preference-senstive surgery decorators')

    inpatient_surgery = _collect_pss_eligible_ip_surg(
        dfs_input['outclaims'].where(
            spark_funcs.col('mr_line').startswith('I12')
            )
        )

    outpatient_surgery = _collect_pss_eligible_op_surg(
        dfs_input['outclaims'].where(
            spark_funcs.col('mr_line').startswith('O12')
            )
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
            **kwargs,
            )

#def import_reference_file(
#        sparkapp: SparkApp,
#        path_ref: Path,
#        **kwargs_read_csv
#    ) -> pyspark.sql.DataFrame:
#    _schema = prm.spark.io_txt.build_structtype_from_csv(
#        path_ref.parent / '{}_schema.csv'.format(path_ref.stem),
#    )
#    _df = sparkapp.session.read.csv(
#        str(path_ref),
#        schema=_schema,
#        **kwargs_read_csv,
#        )
#    return _df
##
#paths_local_refs = {
#PATH_LOCAL_REFS / 'ref_CCS113_turp.csv',
#PATH_LOCAL_REFS / 'ref_CCS124_hysterectomy.csv',
#PATH_LOCAL_REFS / 'ref_CCS149_arthroscopy.csv',
#PATH_LOCAL_REFS / 'ref_CCS152_arthroplasty_knee.csv',
#PATH_LOCAL_REFS / 'ref_CCS153_hip_replacement.csv',
#PATH_LOCAL_REFS / 'ref_CCS154_arthroplasty_other.csv',
#PATH_LOCAL_REFS / 'ref_CCS158_spinal_fusion.csv',
#PATH_LOCAL_REFS / 'ref_CCS244_gastric_bypass.csv',
#PATH_LOCAL_REFS / 'ref_CCS3_laminectomy.csv',
#PATH_LOCAL_REFS / 'ref_CCS44_cabg.csv',
#PATH_LOCAL_REFS / 'ref_CCS45_ptca.csv',
#PATH_LOCAL_REFS / 'ref_CCS48_pacemaker_defibrillator.csv',
#PATH_LOCAL_REFS / 'ref_CCS51thru61_vessel.csv',
#PATH_LOCAL_REFS / 'ref_CCS84_cholecystectomy.csv',
#    }
#dfs_refs = {
#    path.stem: sparkapp.session.read.csv(
#        str(path),
#        header=True,
#        )
#    for path in paths_local_refs
#    }
#dfs_input = {
#    'outclaims': sparkapp.load_df(
#        META_SHARED[73, 'out'] / 'outclaims_prm.parquet'
#        ).select(
#            '*',
#            spark_funcs.col('prm_prv_id_ccn').alias('prv_id_ccn')
#            ),
#}
#
#refs_all = dfs_refs['ref_CCS149_arthroscopy'].union(dfs_refs['ref_CCS113_turp']).union(dfs_refs['ref_CCS45_ptca']).union(dfs_refs['ref_CCS158_spinal_fusion']).union(dfs_refs['ref_CCS44_cabg']).union(dfs_refs['ref_CCS153_hip_replacement']).union(dfs_refs['ref_CCS154_arthroplasty_other']).union(dfs_refs['ref_CCS84_cholecystectomy']).union(dfs_refs['ref_CCS152_arthroplasty_knee']).union(dfs_refs['ref_CCS3_laminectomy']).union(dfs_refs['ref_CCS244_gastric_bypass']).union(dfs_refs['ref_CCS51thru61_vessel']).union(dfs_refs['ref_CCS124_hysterectomy']).union(dfs_refs['ref_CCS48_pacemaker_defibrillator'])