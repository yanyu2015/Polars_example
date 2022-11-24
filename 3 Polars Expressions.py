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

#%%% 二进制函数和修改
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
        pl.col("*"),  # select all，即输出结果的前4列
        pl.col("random").sum().over("groups").alias("sum[random]/groups"),
        # 即按roups列分组，再对random列进行组内求和，注意顺序
        # 然后再给别名
        pl.col("random").list().over("names").alias("random/name"),
    ]
]
out

#%% 3.2 Expression contexts
#%%% context
"""
A Series of a length of 1 will be broadcasted
   to match the height of the DataFrame.
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

#%%% 添加新列 df.with_columns
out = df.with_columns(
    [
        pl.sum("nrs").alias("nrs_sum"),
        pl.col("random").count().alias("count"),
    ]
)
out
# 示例代码有误

#%%% 分组上下文df.groupby().agg()
out = df.groupby("groups").agg(
    [
        pl.sum("nrs"),  
        # sum nrs by groups，对nrs分组求和
        pl.col("random").count().alias("count"),  
        # count group members
        # 对random分组计数

        pl.col("random").filter(
            pl.col("names").is_not_null()
        ).sum().suffix("_sum"),
        # sum random where name != null
        # 如果name值不为空，就选出random的值，
        # 然后求和，并添加后缀名
        
        pl.col("names").reverse().alias(("reversed names")),
        # 反向排序
    ]
)
out


#%% 3.3 GroupBy分组
# ### 导入数据
# 说明，数据来自于 https://github.com/unitedstates/congress-legislators  
# 但是直接下载整个包没啥数据
# 我是在页面中选择
# |File|Download|Description|
# |:----|:----|:----|
# |legislators-current|YAML JSON CSV|Currently serving Members of Congress.|
# 
# 
# 然后下载了其中的CSV文件
# 
# 用这个工具
# https://markdown-convert.com/zh/tool/table
# 
# %%
dataset = pl.read_csv('./dataset/legislators-current.csv')
dataset.head(10)
dataset.columns

# %%
q = (
    dataset.lazy()
    .groupby("first_name") #按first_name列分组，即第2列
    .agg(
        [
            pl.count(),               #组内计数
            pl.col("gender"),         #组内值列表化 .list()有没有都一样
            pl.first("last_name"),    #last_name列组内的第一个值
        ]
    )
    .sort("count", reverse=True)      #按上面聚合得到的count函数排序
    .limit(5)                         #只取结果的前5行
)
# type(q)
# polars.internals.lazy_frame.LazyFrame
# 所以通过collect函数得到
df = q.collect()
df

#%%% GroupBy 条件句
# 假设我们想知道一个“州”有多少代表是民主党或共和党。  
# 然后按Republican降序排序

# %%
q = (
    dataset.lazy()
    .groupby("state")
    .agg(
        [
            (
                pl.col("party") == "Democrat"
            ).sum().alias("Democrat"),
            (
                pl.col("party") == "Republican"
            ).sum().alias("Republican"),
        ]
    )
    .sort("Republican", reverse=True)
    .limit(10)
)

df = q.collect()
df

# %% [markdown]
# 上述代码的另一种实现方式：嵌套分组  
# 这个逻辑我不是很喜欢
q = (
    dataset.lazy()
    .groupby(["state", "party"])
    .agg( 
            [pl.count("party").alias("count")]
        )    # 至此已经完成分组，并设定要求为计数
    .filter( # 设定在上面的基础上的计数对象
        (pl.col("party") == "Democrat") | 
            (pl.col("party") == "Republican")
    )
    .sort("count", reverse=True)
    .limit(10)
)

df = q.collect()
df

# %%% 过滤
# 在解决问题之前，学下时间处理的知识  
# 否则代码会报错  
# 下面的代码，我们将date列转换为时间格式

# %%
dated = pl.DataFrame(
    {
        "date": ["2020-01-02", "2020-01-03", "2020-01-04"], 
        "index": [1, 2, 3]
    }
)

# 注意，这里with_column比较好用
# 本来是新增一列的，但是，如果我们没有使用alias，则会是替换掉原来的同名列
q = dated.lazy().with_column(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("b_squared")
    )

df = q.collect()
df

# %% [markdown]
# 如果使用select，则应书写如下
q = dated.lazy().select(
        [
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col('index'),
        ]
    )

df = q.collect()
df
df.dtypes
# polars.datatypes.Date

# %% [markdown]
# 下面就是年月日时分秒的形式  
# 其中format可以查看datetime的api或者rust的api
dated = pl.DataFrame(
    {
        "date": ["2020-01-02 01:11:11", "2020-01-03 02:22:22", "2020-01-04 03:33:33"], 
        "index": [1, 2, 3]
    }
)

q = dated.lazy().with_column(
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    )

df = q.collect()
df

# %%
q = dataset.lazy().with_column(
        pl.col("birthday").str.strptime(pl.Date, "%Y-%m-%d")
    )

datasetn = q.collect()
datasetn.head(3)

# %%
from datetime import date

# 计算截止到2021.1.1的年龄
def compute_age() -> pl.Expr:
    # 函数后面跟着的-> pl.Expr是函数返回值的类型建议符，
    # 用来说明该函数返回的值是什么类型。
    # 返回出生日期距离2021,。1.1的天数
    return date(2021, 1, 1).year - pl.col("birthday").dt.year()

# 计算性别=gender的组内均值
def avg_birthday(gender: str) -> pl.Expr:
    return compute_age().filter(
            pl.col("gender") == gender
        ).mean().alias(f"avg {gender} birthday")


q = (
    datasetn.lazy()
    .groupby(["state"],maintain_order=True)
    .agg(
        [
            avg_birthday("M"), #女性年龄均值
            avg_birthday("F"),
            (pl.col("gender") == "M").count().alias("# male"), #女性的数量
            (pl.col("gender") == "F").sum().alias("# female"),
        ]
    )
)
# 卡住我了。
# 1 为啥要对lpsum
# 2 为啥每次输出的结果在随机变化，是因为并行了吧
df = q.collect()
df

# %% [markdown]
# 由于并行的原因，结果每次都不通  
# 所以如果需要每次结果都相同，则要使用下面的语句
# > 
# > ```python
# > .groupby(["state"],maintain_order=True)
# > ```
# 
# 但是这会增加计算时间的开销


#%%% 排序
# 假设我们想得到每个州最年长和最年轻的政治家的名字。
def get_person() -> pl.Expr:
    return pl.col("first_name") + pl.lit(" ") + pl.col("last_name")

#
q = (
    dataset.lazy()
    .sort("birthday")
    .groupby(["state"])
    .agg(
        [
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
        ]
    )
    .limit(5)
)

df = q.collect()
df

# %% [markdown]
# 如果我们还想按字母顺序对名称进行排序
def get_person() -> pl.Expr:
    return pl.col("first_name") + pl.lit(" ") + pl.col("last_name")


q = (
    dataset.lazy()
    .sort("birthday")
    .groupby(["state"])
    .agg(
        [
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
            get_person().sort().first().alias("alphabetical_first"),
        ]
    )
    .limit(5)
)

df = q.collect()
df

# %% [markdown]
# 我们甚至可以按groupby上下文中的另一列进行排序。 
# 如果我们还想知道按字母顺序排列的第一个名字名字对应的人是男性还是女性

# %%
def get_person() -> pl.Expr:
    return pl.col("first_name") + pl.lit(" ") + pl.col("last_name")


q = (
    dataset.lazy()
    .sort("birthday")
    .groupby(["state"])
    .agg(
        [
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
            get_person().sort().first().alias("alphabetical_first"),
            pl.col("gender").sort_by("first_name").first().alias("gender"),
        ]
    )
    .sort("state")
    .limit(5)
)

df = q.collect()
df


# 注意：  
# sort是对DataFrame操作的  
# 而sort_by是对column操作的，其实也是联动排序







