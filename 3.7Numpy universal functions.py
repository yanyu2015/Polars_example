# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:26:23 2022

@author: xuanQS
"""


import polars as pl


"""
Polars expressions support NumPy ufuncs. 
See here for a list on all supported numpy functions.

This means that if a function is not provided by Polars, 
    we can use NumPy and we still 
        have fast columnar operation through the NumPy API.
"""
#%% Example 使用np.log求自然对数
import polars as pl
import numpy as np

df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
df
out = df.select(
    [
        np.log(pl.all())
        .suffix("_log"),
    ]
)
print(out)


