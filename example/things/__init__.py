from django import VERSION


if VERSION[0:2] <= (1, 5):
    from .tests import *  # noqa
