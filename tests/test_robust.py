import numpy as np
import pytest

from sentinel_lens.detectors.robust import robust_z

NAN = np.nan


def test_median_5_mad_1_example():
    past = np.array([[3, 4, 4, 5, 6, 6, 7]], dtype=float).T          # one value, seven history entries
    expected, wobble, z, n_used = robust_z(np.array([9.45]), past, floor=0)
    assert expected[0] == 5 and wobble[0] == pytest.approx(1.4826) and n_used[0] == 7
    assert z[0] == pytest.approx(4.45 / 1.4826)


def test_each_value_uses_only_its_own_column_and_skips_nan():
    past = np.array([[10, NAN], [10, NAN], [NAN, NAN]], dtype=float)
    expected, wobble, z, n_used = robust_z(np.array([11.0, 5.0]), past, floor=0.05, min_history=2)
    assert list(n_used) == [2, 0]
    assert expected[0] == 10 and wobble[0] == 0.5 and z[0] == pytest.approx(2.0)   # MAD 0 -> the 5% floor
    assert np.isnan(z[1])                                                           # no history, no score


def test_min_history_withholds_the_score():
    past = np.array([[10.0], [10.0]])
    assert np.isnan(robust_z(np.array([20.0]), past, min_history=3)[2][0])
    assert robust_z(np.array([20.0]), past, min_history=2)[2][0] == pytest.approx(20.0)
