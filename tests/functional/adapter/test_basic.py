from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)


class TestSingularTestsInfer(BaseSingularTests):
    pass


class TestSingularTestsEphemeralInfer(BaseSingularTestsEphemeral):
    pass


class TestEmptyInfer(BaseEmpty):
    pass
