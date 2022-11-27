# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:22:07 2022

@author: xuanQS
"""
import polars as pl


#%% Manual Sum
df = pl.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [10, 20, 30],
    }
)
out = df.select(
    pl.fold(acc=pl.lit(0), f=lambda acc, x: acc + x, exprs=pl.col("*")).alias("sum"),
)
print(out)

#%% Conditional
df = pl.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [0, 1, 2],
    }
)

out = df.filter(
    pl.fold(
        acc=pl.lit(True),
        f=lambda acc, x: acc & x,
        exprs=pl.col("*") > 1,
    )
)
print(out)


#%% Folds and string data
df = pl.DataFrame(
    {
        "a": ["a", "b", "c"],
        "b": [1, 2, 3],
    }
)

out = df.select(
    [
        pl.concat_str(["a", "b"]),
    ]
)
print(out)



