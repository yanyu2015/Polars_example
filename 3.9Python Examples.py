# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:27:03 2022

@author: xuanQS
"""
import polars as pl

#%% Expressions
df = pl.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        "optional": [28, 300, None, 2, -30],
    }
)
df

#%%  Selection context
# We can select by name

(df.select([
    pl.col("A"),
    "B",      # the col part is inferred
    pl.lit("B"),  # we must tell polars we mean the literal "B"
    pl.col("fruits"),
]))

# you can select columns with a regex if it starts with '^' and ends with '$'

(df.select([
    pl.col("^A|B$").sum()
]))

# you can select multiple columns by name

(df.select([
    pl.col(["A", "B"]).sum()
]))

# We select everything in normal order
# Then we select everything in reversed order

(df.select([
    pl.all(),
    pl.all().reverse().suffix("_reverse")
]))

# all expressions run in parallel
# single valued `Series` are broadcasted to the shape of the `DataFrame`

(df.select([
    pl.all(),
    pl.all().sum().suffix("_sum")
]))

# there are `str` and `dt` namespaces for specialized functions

predicate = pl.col("fruits").str.contains("^b.*")

(df.select([
    predicate
]))

# use the predicate to filter

df.filter(predicate)

# predicate expressions can be used to filter

(df.select([
    pl.col("A").filter(pl.col("fruits").str.contains("^b.*")).sum(),
    (pl.col("B").filter(pl.col("cars").str.contains("^b.*")).sum() * pl.col("B").sum()).alias("some_compute()"),
]))


# We can do arithmetic on columns and (literal) values
# can be evaluated to 1 without programmer knowing

some_var = 1

(df.select([
    ((pl.col("A") / 124.0 * pl.col("B")) / pl.sum("B") * some_var).alias("computed")
]))


# We can combine columns by a predicate

(df.select([
    "fruits",
    "B",
    pl.when(pl.col("fruits") == "banana").then(pl.col("B")).otherwise(-1).alias("b")
]))

# We can combine columns by a fold operation on column level

(df.select([
    "A",
    "B",
    pl.fold(0, lambda a, b: a + b, [pl.col("A"), "B", pl.col("B")**2, pl.col("A") / 2.0]).alias("fold")
]))

# even combine all

(df.select([
    pl.arange(0, df.height).alias("idx"),
    "A",
    pl.col("A").shift().alias("A_shifted"),
    pl.concat_str(pl.all(), "-").alias("str_concat_1"),  # prefer this
    pl.fold(pl.col("A"), lambda a, b: a + "-" + b, pl.all().exclude("A")).alias("str_concat_2"),  # over this (accidentally O(n^2))
]))


#%% Aggregation context
# we can still combine many expressions

(df.sort("cars").groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        pl.sum("B").alias("B_sum2"),  # syntactic sugar for the first
        pl.first("fruits").alias("fruits_first"),
        pl.count("A").alias("count"),
        pl.col("cars").reverse()
    ]))


# We can explode the list column "cars"

(df.sort("cars").groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        pl.sum("B").alias("B_sum2"),  # syntactic sugar for the first
        pl.first("fruits").alias("fruits_first"),
        pl.count("A").alias("count"),
        pl.col("cars").reverse()
    ])).explode("cars")


(df.groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        pl.sum("B").alias("B_sum2"),  # syntactic sugar for the first
        pl.first("fruits").alias("fruits_first"),
        pl.count(),
        pl.col("B").shift().alias("B_shifted")
    ])
 .explode("B_shifted")
)

# We can explode the list column "cars"

(df.sort("cars").groupby("fruits")
    .agg([
        pl.col("B").sum(),
        pl.sum("B").alias("B_sum2"),  # syntactic sugar for the first
        pl.first("fruits").alias("fruits_first"),
        pl.count("A").alias("count"),
        pl.col("cars").reverse()
    ])).explode("cars")

# we can also get the list of the groups

(df.groupby("fruits")
    .agg([
         pl.col("B").shift().alias("shift_B"),
         pl.col("B").reverse().alias("rev_B"),
    ]))

# we can do predicates in the groupby as well

(df.groupby("fruits")
    .agg([
        pl.col("B").filter(pl.col("B") > 1).list().keep_name(),
    ]))


# and sum only by the values where the predicates are true

(df.groupby("fruits")
    .agg([
        pl.col("B").filter(pl.col("B") > 1).mean(),
    ]))

# Another example

(df.groupby("fruits")
    .agg([
        pl.col("B").shift_and_fill(1, fill_value=0).alias("shifted"),
        pl.col("B").shift_and_fill(1, fill_value=0).sum().alias("shifted_sum"),
    ]))



#%% Window functions!
pl.col("foo").aggregation_expression(..).over("column_used_to_group")

# groupby 2 different columns

(df.select([
    "fruits",
    "cars",
    "B",
    pl.col("B").sum().over("fruits").alias("B_sum_by_fruits"),
    pl.col("B").sum().over("cars").alias("B_sum_by_cars"),
]))

# reverse B by groups and show the results in original DF

(df.select([
    "fruits",
    "B",
    pl.col("B").reverse().over("fruits").alias("B_reversed_by_fruits")
]))

# Lag a column within "fruits"

(df.select([
    "fruits",
    "B",
    pl.col("B").shift().over("fruits").alias("lag_B_by_fruits")
]))


#%% 
#%% 
#%% 
#%% 
