# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 23:09:25 2022

@author: xuanQS
"""

import polars as pl
import numpy as np

np.random.seed(12)
df = pl.DataFrame(
    {
        "nrs": [1, 2, 3, None, 5],
        "names": ["foo", "ham", "spam", "egg", None],
        "random": np.random.rand(5),
        "groups": ["A", "A", "B", "C", "B"],
    }
)
df

"""
You cannot use an expression anywhere. 
    An expression needs a context, 
        the available contexts are:

selection: df.select([..])
groupy aggregation: df.groupby(..).agg([..])
hstack/ add columns: df.with_columns([..])
其中的[..]，应该就是所谓的context吧
"""

#%% 3.2 Expression contexts

#%%% Syntactic sugar
"""
The reason for such a context, 
    is that you actually are using the Polars lazy API, 
    even if you use it in eager. For instance this snippet:
对于context而言，看似用即时模式，实则是用lazy模式
df.groupby("foo").agg([pl.col("bar").sum()])
    actually desugars to:
(df.lazy().groupby("foo").agg([pl.col("bar").sum()])).collect()
也就是说，被自动翻译成lazy模式的语法了
This allows Polars to push the expression into 
    the query engine, 
    do optimizations, and cache intermediate results.
    通过查询引擎，做了优化，并缓存了中间结果
Rust differs from Python somewhat in this respect. 
Where Python's eager mode is little more than 
    a thin veneer over the lazy API, 
    Rust's eager mode is closer to an implementation detail, 
    and isn't really recommended for end-user use. 
    It is possible that the eager API in Rust will be 
    scoped private sometime in the future. 
    Therefore, for the remainder of this document, 
    assume that the Rust examples are using the lazy API.

"""

#%%% Select context
#%%%% Select context
"""
The expressions in this context must produce Series that
 are all the same length or have a length of 1.
A Series of a length of 1 will be broadcasted
   to match the height of the DataFrame.
select作用于列
生成的是Series,多个操作的长度要么相等，要么为1
如果多个操作的结果中有一个为长度为1，
就会扩展到这多个操作结果构成的DF的高度（行数）
（多个操作的结果会构建成DF，即out的type是DF）
"""
out = df.select(
    [
        pl.sum("nrs"), #求和，被自动broadcasted
        pl.col("names").sort(),
        pl.col("names").first().alias("first name"), 
        # first()得到第一个元素
        (pl.mean("nrs") * 10).alias("10xnrs"),
    ]
)
out
type(out)
#%%%% 添加新列 df.with_columns
"""
Adding columns to a DataFrame using with_columns 
    is also the selection context.
"""
out = df.with_columns(
    [
        pl.sum("nrs").alias("nrs_sum"),
        pl.col("random").count().alias("count"),
    ]
)
out
# 示例代码有误

#%%% Groupby context
#%%%% 分组上下文df.groupby().agg()
"""
In the groupby context expressions work on groups and
 thus may yield results of any length 
 (a group may have many members).

"""
out = df.groupby("groups").agg(
    [
        pl.sum("nrs"),  
        # sum nrs by groups，对nrs分组求和
        pl.col("random").count().alias("count"),  
        # count group members
        # 对random分组计数

        # sum random where name != null
        pl.col("random").filter(
            pl.col("names").is_not_null()
        ).sum().suffix("_sum"),
        # 1 选出random列
        # 2 如果name值不为空，就选出random的值，
        # 然后求和，并添加后缀名
        
        pl.col("names").reverse().alias(("reversed names")),
        # 反向排序
    ]
)
out

"""
Besides the standard groupby, 
groupby_dynamic, 
and groupby_rolling 
are also entrances to the groupby context.
"""

