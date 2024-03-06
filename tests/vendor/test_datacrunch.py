import unittest

from crunch.vendor import datacrunch
import numpy
import pandas

class GaussianizerTest(unittest.TestCase):

    def test(self):
        series = pandas.Series(numpy.random.rand(100000)) # Central Limit Theorem respected here.
        gauss_series = datacrunch._gaussianizer(series)

        tol = 1e-3

        self.assertAlmostEqual(1.0, gauss_series.corr(series, method='spearman'), delta=tol)
        self.assertAlmostEqual(0.0, gauss_series.mean(), delta=tol)
        self.assertAlmostEqual(1.0, gauss_series.std(), delta=tol)
