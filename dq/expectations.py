from great_expectations.core.expectation_configuration import ExpectationConfiguration


expectations = []

expectations.append(ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
        "column" : "ID",
        "mostly": 1
    },
    meta={

    }
))

expectations.append(ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
        "column" : "Title",
        "mostly": 1
    },
    meta={

    }
))

expectations.append(ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column" : "Title",
        "value_set": ["User", "Task"]
    },
    meta={

    }
))