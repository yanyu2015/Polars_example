# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:22:07 2022

@author: xuanQS
"""
import polars as pl

"""
Polars provides expressions/methods for horizontal aggregations 
    like sum, min, mean, etc. 
    by setting the argument axis=1.
However, when you need a more complex aggregation 
    the default methods provided by the Polars library may not be sufficient. 
That's when folds come in handy.

即默认的agg配合sum，min，mean等等，对于复杂的agg捉襟见肘
所以就要用folds

The Polars fold expression operates on columns for maximum speed. 
It utilizes the data layout very efficiently and 
    often has vectorized execution.
即向量化了
"""
#%% Manual Sum
# Let's start with an example by
# implementing the sum operation ourselves, with a fold.
# 自实现sum
df = pl.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [10, 20, 30],
    }
)
out = df.select(
    pl.fold(acc=pl.lit(0), 
            f=lambda acc,x: acc + x, 
            exprs=pl.col("*"))
    .alias("sum"),
)
print(out)

"""
The snippet above recursively applies the function f(acc, x) -> acc to
    an accumulator acc and a new column x. 
The function operates on columns individually and 
    can take advantage of cache efficiency and vectorization.
查询API可知，pl.fold接收3个参数
acc
    Accumulator Expression. 
    This is the value that will be initialized 
    when the fold starts. 
    For a sum this could for instance be lit(0).

f
    Function to apply over the accumulator and the value. 
    Fn(acc, value) -> new_value

exprs
    Expressions to aggregate over. 
    May also be a wildcard(通配符) expression.

可见第一个参数是初始值，这里就是一个特数字字面量0
第二个参数
    是一个匿名函数，所以这里
第二个参数
    f=lambda acc,x: acc + x
    一整个就是一个匿名函数，只不过这里有两个参数而已
    这个匿名函数，会对每一列递归调用
    即第一次是 0+ 第一列（acc是0，x是第一列）
    即第二次是 0+第一列 + 第二列（acc是0+第一列，x是第二列）
第三个参数是
    代表作用到哪些列。这里是选择所有列（以通配符的方式）
"""

#%% Conditional，有条件地使用folds
df = pl.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [0, 1, 2],
    }
)
df
out = df.filter(
    pl.fold(
        acc=pl.lit(True),
        f=lambda acc, x: acc & x,
        exprs=pl.col("*") > 1,
    )
)
print(out)
# 通过exprs=pl.col("*") > 1
# we filter all rows where each column value is > 1
# 注意，这个大于1，是要求某一行的所有元素都满足大于1



#%% concat_str方法【拼接字符串
# 字符串拼接虽然可以使用folds实现
# 但是由于复杂度是平方级的
# 所以建议使用concat_str方法
df = pl.DataFrame(
    {
        "a": ["a", "b", "c"],
        "b": [1, 2, 3],
    }
)
df
out = df.select(
    [
        pl.concat_str(["a", "b"]),
    ]
)
print(out)



