from spark_apps.parameters import create_spark_args
from spark_apps.cohorts.spark_app_base import create_prediction_cohort
from spark_apps.cohorts.query_builder import QueryBuilder, QuerySpec

DEPRESSION_QUERY = """
select distinct vo.person_id, vo.visit_occurrence_id, c.cohort_start_date as index_date, c.cohort_end_date from global_temp.dylee_cohort c join global_temp.visit_occurrence vo on c.subject_id = vo.person_id and c.cohort_start_date = vo.visit_start_date where c.cohort_definition_id = 389
"""

DEPRESSION_COHORT = 'dylee_depression_cohort'

SUICIDE_QUERY = """
select distinct vo.person_id, vo.visit_occurrence_id, c.cohort_start_date as index_date, c.cohort_end_date from global_temp.dylee_cohort c join global_temp.visit_occurrence vo on c.subject_id = vo.person_id and c.cohort_start_date = vo.visit_start_date  where c.cohort_definition_id = 2382
"""

SUICIDE_COHORT = 'dylee_suicide_cohort'

DEPENDENCY_LIST = ['visit_occurrence', 'dylee_cohort']
DOMAIN_TABLE_LIST = ['condition_occurrence', 'drug_exposure', 'procedure_occurrence', 'observation']


def main(spark_args):
    depression_query = QuerySpec(table_name=DEPRESSION_COHORT,
                                   query_template=DEPRESSION_QUERY,
                                   parameters={})
    depression_population = QueryBuilder(cohort_name=DEPRESSION_COHORT,
                                dependency_list=DEPENDENCY_LIST,
                                query=depression_query)

    suicide_query = QuerySpec(table_name=SUICIDE_COHORT,
                                      query_template=SUICIDE_QUERY,
                                      parameters={})
    suicide_population = QueryBuilder(cohort_name=SUICIDE_COHORT,
                                   dependency_list=DEPENDENCY_LIST,
                                   query=suicide_query)

    create_prediction_cohort(spark_args,
                             depression_population,
                             suicide_population,
                             DOMAIN_TABLE_LIST)


if __name__ == '__main__':
    main(create_spark_args())
