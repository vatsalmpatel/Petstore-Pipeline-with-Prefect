from src.main import clean_stats
# from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness

def test_clean_stats_data():
    test_input = {
        "sold": 8,
        "{PetStatus-Updated}": 1,
        "string": 758,
        "pending": 1,
        "Pending": 1,
        "available": 227
    }

    with prefect_test_harness():
        result_output = clean_stats.fn(test_input)
        assert result_output == {
            "sold": 8,
            "pending": 2,
            "available": 227,
            "unavailable": 0
        }