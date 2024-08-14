from airflow.decorators import task

db_pipeline_id_to_path_dict = {
    "pipelines/mortgage_analytics": "dbfs:/FileStore/prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/mortgage_analytics-1.0-py3-none-any.whl", 
    "pipelines/troubled_mortgage_assistance": "dbfs:/FileStore/prophecy/artifacts/saas/app/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/troubled_mortgage_HAF-1.0-py3-none-any.whl"
}


def task_wrapper(task_id):

    def decorator(func):

        @task(task_id = task_id)
        def wrapper(*args, **context):
            ## running the actual method.
            return func(*args, **context).execute(context)

        return wrapper

    return decorator

pipeline_package_name = {
    "pipelines/mortgage_analytics": "mortgage_analytics", 
    "pipelines/troubled_mortgage_assistance": "troubled_mortgage_HAF"
}
