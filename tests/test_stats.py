import pytest

from sentinel_lens.stats import wilson_interval


def test_wilson_known_values():
    lo, hi = wilson_interval(9, 10)
    assert lo == pytest.approx(0.596, abs=0.001) and hi == pytest.approx(0.982, abs=0.001)
    lo, hi = wilson_interval(5, 10)
    assert lo == pytest.approx(0.237, abs=0.001) and hi == pytest.approx(0.763, abs=0.001)


def test_wilson_stays_inside_zero_and_one_at_the_extremes():
    lo, hi = wilson_interval(10, 10)
    assert lo == pytest.approx(0.722, abs=0.001) and hi == 1.0      # all detected is still not "certainly 100%"
    lo, hi = wilson_interval(0, 10)
    assert lo == 0.0 and hi == pytest.approx(0.278, abs=0.001)


def test_wilson_narrows_with_more_data_and_handles_no_data():
    assert wilson_interval(90, 100)[1] - wilson_interval(90, 100)[0] < wilson_interval(9, 10)[1] - wilson_interval(9, 10)[0]
    assert wilson_interval(0, 0) == (0.0, 1.0)
    with pytest.raises(ValueError):
        wilson_interval(11, 10)
