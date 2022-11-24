"""
Created on Mon Nov 21
@author: xuanQS
"""
"""
Polars expressions are a mapping from a series to a series 
    (or mathematically Fn(Series) -> Series).
"""
import polars as pl
import numpy as np

df = pl.read_csv("./dataset/iris.csv")
df

pl.col("sepal_length").sort().head(2)
# 1 选择sepal_length列
# 对选出来的列排序
# 出去前2行

"""
every expression produces a new expression, 
    and that they can be piped together. 
即，表达式返回表达式，可以使用管道流
"""
# =============================================================================
# df.select([
#     pl.col("foo").sort().head(2),
#     pl.col("bar").filter(pl.col("foo") == 1).sum()
# ])
# 
"""
All expressions are run in parallel
这就很有意思了
"""
# =============================================================================

#%% 3.1 Expression examples
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

#%%% 求唯一值
out = df.select(
    [
        pl.col("names").n_unique().alias("unique_names_1"),
        # 计算name列中的唯一值，并把结果存为名为unique_names_1的列中
        pl.col("names").unique().count().alias("unique_names_2"),
    ]
)
# 稍微查看了下api，其实这两个方法的功能是一样的
# 所以结果也一样，不知道为啥提供两个
out

#%%% 各种聚合
out = df.select(
    [
        pl.sum("random").alias("sum"), 
        #对random列求和，并将结果存在sum列中
        pl.min("random").alias("min"),
        pl.max("random").alias("max"),
        pl.col("random").max().alias("other_max"),
        pl.std("random").alias("std dev"),
        pl.var("random").alias("variance"),
    ]
)
out

#%%% 过滤和条件
out = df.select(
    [
        pl.col("names").filter( #选出names列，然后按条件筛选
            pl.col("names").str.contains(r"am$")  
            # 条件为该列的值以am字符结尾的行
            # $表示结尾
        ).count(),
    ]
)
out

#%%% 二选一函数和修改
"""
In the example below   
we use a conditional to create a new expression in the following  
    `when -> then -> otherwise` construct.  
    The `when` function requires a predicate expression(断言，可推导表达式)
        (and thus leads to a boolean Series).   
    The `then` function expects an expression that 
        will be used in case the predicate evaluates to true,  
    and the `otherwise` function expects an expression that 
        will be used in case the predicate evaluates to false.
"""
out = df.select(
    [
        pl.when(
            pl.col("random") > 0.5
        ).then(0).otherwise( pl.col("random") ) * pl.sum("nrs"),
        #如果random的值大于0.5，则取0
        #否则，令值为取出的random的值
        #所有的结果乘以——pl.sum("nrs")，即11
    ]
)
out
# 注意，直接打印pl.sum("nrs")是没用的
# 因为这只是一个表达式
pl.sum("nrs")
# 只有借助select才行
df.select(
    pl.sum("nrs")
)

#%%% 窗口表达式
"""
A polars expression can also do an implicit 
    GROUPBY, AGGREGATION, and JOIN in a single expression.   
In the examples below we do a GROUPBY OVER "groups" 
    and AGGREGATE SUM of "random",   
    即，按groups列分组，然后对random列进行组内求和  
and in the next expression 
    we GROUPBY OVER "names" and AGGREGATE a LIST of "random".   
    按name列分组，然后对random列转为list  
These window functions can be combined with other expressions 
    and are an efficient way to determine group statistics.   
See more on those group statistics here.
"""
out = df[
    [
        pl.col("*"),  # select all，即输出结果的所有列
        pl.col("random").sum().over("groups").alias("sum[random]/groups"),
        # 即按groups列分组，再对random列进行组内求和
        # 注意顺序，是要先读over，再读pl.col和sum，这是英语语法习惯
        # 然后再给别名
        pl.col("random").list().over("names").alias("random/name"),
    ]
]
out