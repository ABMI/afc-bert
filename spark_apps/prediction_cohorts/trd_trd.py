from spark_apps.parameters import create_spark_args
from spark_apps.cohorts.spark_app_base import create_prediction_cohort
from spark_apps.cohorts.query_builder import QueryBuilder, QuerySpec

DEPRESSION_QUERY = """
select distinct vo.person_id, vo.visit_occurrence_id, c.cohort_start_date as index_date, c.cohort_end_date from global_temp.dylee_cohort c join global_temp.visit_occurrence vo on c.subject_id = vo.person_id and c.cohort_start_date = vo.visit_start_date where c.cohort_definition_id = 389
"""

DEPRESSION_COHORT = 'dylee_depression_cohort'

TRD_QUERY = """
select distinct vo.person_id, vo.visit_occurrence_id, c.cohort_start_date as index_date, c.cohort_end_date from global_temp.dylee_cohort c join global_temp.visit_occurrence vo on c.subject_id = vo.person_id and c.cohort_start_date = vo.visit_start_date  where c.cohort_definition_id = 388
"""

TRD_COHORT = 'dylee_trd_cohort'

DEPENDENCY_LIST = ['visit_occurrence', 'dylee_cohort']
DOMAIN_TABLE_LIST = ['condition_occurrence', 'drug_exposure', 'procedure_occurrence']


def main(spark_args):
    depression_query = QuerySpec(table_name=DEPRESSION_COHORT,
                                   query_template=DEPRESSION_QUERY,
                                   parameters={})
    depression_population = QueryBuilder(cohort_name=DEPRESSION_COHORT,
                                dependency_list=DEPENDENCY_LIST,
                                query=depression_query)

    trd_query = QuerySpec(table_name=TRD_COHORT,
                                      query_template=TRD_QUERY,
                                      parameters={})
    trd_population = QueryBuilder(cohort_name=TRD_COHORT,
                                   dependency_list=DEPENDENCY_LIST,
                                   query=trd_query)

    create_prediction_cohort(spark_args,
                             depression_population,
                             trd_population,
                             DOMAIN_TABLE_LIST)


if __name__ == '__main__':
    main(create_spark_args())
