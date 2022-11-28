# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:26:23 2022

@author: xuanQS
"""


import polars as pl

#%% Example
import polars as pl
import numpy as np

df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

out = df.select(
    [
        np.log(pl.all()).suffix("_log"),
    ]
)
print(out)


