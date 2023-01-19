
import great_expectations as gx
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, AnonymizedUsageStatisticsConfig
from great_expectations.data_context import AbstractDataContext, FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest

def init_gx() -> FileDataContext:
    data_context_config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,
        datasources={
            "spark_runtime_datasource": DatasourceConfig(
                class_name="Datasource",
                execution_engine={
                    "class_name": "SparkDFExecutionEngine"
                },
                data_connectors={
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"],
                    },
                },
            )
        },
        stores={
            "expectations_FS_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "root_directory": "D:\\Github\\d",
                    "base_directory": "store\\expectations"
                },
            },
            "validations_FS_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "root_directory": "D:\\Github\\d",
                    "base_directory": "store\\validations"
                },
            },
            "checkpoints_FS_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "root_directory": "D:\\Github\\d",
                    "base_directory": "store\\checkpoints"
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        },
        expectations_store_name="expectations_FS_store",
        validations_store_name="validations_FS_store",
        checkpoint_store_name="checkpoints_FS_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "root_directory": "D:\\Github\\d",
                    "base_directory":  "data-docs"
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
            }
        },
        validation_operators={
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                    },
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    },
                ],
            }
        },
        anonymous_usage_statistics=AnonymizedUsageStatisticsConfig(enabled=False)
    )
    return gx.get_context(project_config=data_context_config)

def create_batch(context, dataset_name:str, df, expectation_suite_name:str) -> RuntimeBatchRequest:
    batch_request = RuntimeBatchRequest(
        datasource_name="spark_runtime_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=dataset_name,  # this can be anything that identifies this data_asset for you
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default_identifier"}
    )
    checkpoint_name = f"{dataset_name}_ckpnt"
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "expectation_suite_name": expectation_suite_name
    }
    context.add_checkpoint(**checkpoint_config)
    return batch_request, checkpoint_name

def create_checkpoint(context, checkpoint_name, expectation_suite_name):
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "expectation_suite_name": expectation_suite_name
    }
    context.add_checkpoint(**checkpoint_config)

def run_gx(context, dataset_name:str, df):
    checkpoint_name = f"{dataset_name}_ckpnt"
    batch = create_batch(context, dataset_name, df, checkpoint_name)
    results = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[
            {"batch_request": batch},
        ],
    )