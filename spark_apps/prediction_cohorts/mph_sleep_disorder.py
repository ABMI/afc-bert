from spark_apps.parameters import create_spark_args
from spark_apps.cohorts.spark_app_base import create_prediction_cohort
from spark_apps.cohorts.query_builder import QueryBuilder, QuerySpec

MPH_QUERY = """
select distinct vo.person_id, vo.visit_occurrence_id, c.cohort_start_date as index_date, c.cohort_end_date from global_temp.CKim_cohort c join global_temp.visit_occurrence vo on c.subject_id = vo.person_id and c.cohort_start_date = vo.visit_start_date where c.cohort_definition_id = 3558
"""

MPH_COHORT = 'CKim_mph_cohort'

SLEEP_DISORDER_QUERY = """
select distinct vo.person_id, vo.visit_occurrence_id, c.cohort_start_date as index_date, c.cohort_end_date from global_temp.CKim_cohort c join global_temp.visit_occurrence vo on c.subject_id = vo.person_id and c.cohort_start_date = vo.visit_start_date  where c.cohort_definition_id = 3557
"""

SLEEP_DISORDER_COHORT = 'CKim_sleep_disorder_cohort'

DEPENDENCY_LIST = ['visit_occurrence', 'CKim_cohort']
DOMAIN_TABLE_LIST = ['condition_occurrence', 'drug_exposure', 'procedure_occurrence']


def main(spark_args):
    mph_query = QuerySpec(table_name=MPH_COHORT,
                                   query_template=MPH_QUERY,
                                   parameters={})
    mph_population = QueryBuilder(cohort_name=MPH_COHORT,
                                dependency_list=DEPENDENCY_LIST,
                                query=mph_query)

    sleep_disorder_query = QuerySpec(table_name=SLEEP_DISORDER_COHORT,
                                      query_template=SLEEP_DISORDER_QUERY,
                                      parameters={})
    sleep_disorder_population = QueryBuilder(cohort_name=SLEEP_DISORDER_COHORT,
                                   dependency_list=DEPENDENCY_LIST,
                                   query=sleep_disorder_query)

    create_prediction_cohort(spark_args,
                             mph_population,
                             sleep_disorder_population,
                             DOMAIN_TABLE_LIST)


if __name__ == '__main__':
    main(create_spark_args())
