import great_expectations as gx
from dq.core import *
from dq.expectations import expectations


# Import PySpark
import pyspark
from pyspark.sql import SparkSession


dataset_name = "tst"
expectation_suite_name = f"{dataset_name}_expectation_suite"
context: FileDataContext = init_gx()

suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

for e in expectations:
    suite.add_expectation(e)

context.save_expectation_suite(suite)

#Create SparkSession
spark = SparkSession.builder.master("local[16]").appName("explore-gx").getOrCreate()
df = spark.read.options(header=True, inferSchema=True).csv("./data/CloudAdoption.csv")

batch, checkpoint_name = create_batch(context, dataset_name, df, expectation_suite_name)

results = context.run_checkpoint(
    checkpoint_name=checkpoint_name,
    batch_request=batch,
    expectation_suite_name=expectation_suite_name,
    result_format={
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["ID"],
        "return_unexpected_index_query": True,
    }
)

print(type(results))

print(f'''
    __________________________________
    {results}
''')

