# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:26:41 2022

@author: xuanQS
"""
import polars as pl

#%% To map or to apply.
df = pl.DataFrame(
    {
        "keys": ["a", "a", "b"],
        "values": [10, 7, 1],
    }
)

out = df.groupby("keys", maintain_order=True).agg(
    [
        pl.col("values").map(lambda s: s.shift()).alias("shift_map"),
        pl.col("values").shift().alias("shift_expression"),
    ]
)
print(df)

# =============================================================================
# "a" -> [10, 7]
# "b" -> [1]
# 
# 
# "a" -> [null, 10]
# "b" -> [null]
# =============================================================================


print(out)


#%% To apply
out = df.groupby("keys", maintain_order=True).agg(
    [
        pl.col("values").apply(lambda s: s.shift()).alias("shift_map"),
        pl.col("values").shift().alias("shift_expression"),
    ]
)
print(out)


#%% apply in the select context

#%%% Adding a counter
counter = 0


def add_counter(val: int) -> int:
    global counter
    counter += 1
    return counter + val


out = df.select(
    [
        pl.col("values").apply(add_counter).alias("solution_apply"),
        (pl.col("values") + pl.arange(1, pl.count() + 1)).alias("solution_expr"),
    ]
)
print(out)


#%%% Combining multiple column values
[
    {"keys": "a", "values": 10},
    {"keys": "a", "values": 7},
    {"keys": "b", "values": 1},
]

out = df.select(
    [
        pl.struct(["keys", "values"]).apply(lambda x: len(x["keys"]) + x["values"]).alias("solution_apply"),
        (pl.col("keys").str.lengths() + pl.col("values")).alias("solution_expr"),
    ]
)
print(out)

#%% Return types?



