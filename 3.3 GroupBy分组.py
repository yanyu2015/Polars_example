# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 23:10:39 2022

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


