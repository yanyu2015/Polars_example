# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:24:48 2022

@author: xuanQS
"""

import polars as pl

"""
Window functions are expressions with superpowers. 
They allow you to perform aggregations on groups in the select context. 
Let's get a feel of what that means. 
First we create a dataset. 
The dataset loaded in the snippet below contains information about pokemon and
 has the following columns:
['#', 'Name', 'Type 1', 'Type 2', 'Total', 'HP', 
 'Attack', 'Defense', 'Sp. Atk', 'Sp. Def', 
 'Speed', 'Generation', 'Legendary']
"""
# then let's load some csv data with information about pokemon
# df = pl.read_csv(
#     "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv"
# )
# df.write_csv('./dataset/3.5.csv')

# 读入精灵宝可梦的数据
df = pl.read_csv('./dataset/3.5.csv')
#%% Groupby Aggregations in selection
out = df.select(
    [
        "Type 1",
        "Type 2",
        pl.col("Attack").mean().over("Type 1").alias("avg_attack_by_type"),
        pl.col("Defense").mean().over(["Type 1", "Type 2"]).alias("avg_defense_by_type_combination"),
        pl.col("Attack").mean().alias("avg_attack"),
    ]
)
"""
Below we show how to use window functions to 
    group over different columns 
    and perform an aggregation on them. 
Doing so allows us to use multiple groupby operations in parallel, 
    using a single query. 
The results of the aggregation are projected back to 
    the original rows. 
Therefore, a window function will always lead to 
    a DataFrame with the same size as the original.

Note how we call .over("Type 1") and .over(["Type 1", "Type 2"]). 
Using window functions we can aggregate over different groups 
    in a single select call!

The best part is, this won't cost you anything. The computed groups are cached and shared between different window expressions.


这里的over，其实就是基于某一列对另一列求均值等等之类的
但是最前面的       
    "Type 1",
    "Type 2",
有什么用呢？
其实跟如下代码
out = df.select(
    [
        pl.col("Type 1"),
        pl.col("Type 2"),
        pl.col("Attack").mean().over("Type 1").alias("avg_attack_by_type"),
        pl.col("Defense").mean().over(["Type 1", "Type 2"]).alias("avg_defense_by_type_combination"),
        pl.col("Attack").mean().alias("avg_attack"),
    ]
)
是一致的
还有，通过pl.col("Attack").mean().over("Type 1")计算得到的结果
会与pl.col("Type 1")的维度保持一致
所以这里df和out都是163行
"""


#%% Operations per group
"""

Window functions can do more than aggregation. 
They can also be viewed as an operation within a group. 
If, for instance, you want to sort the values within a group, 
    you can write col("value").sort().over("group") 
    即对名为value的列，在组内排序
    and voilà!（瞧） We sorted by group!
Let's filter out some rows to make this more clear.

"""
filtered = df.filter(
    pl.col("Type 2") == "Psychic").select(
    [
        "Name",
        "Type 1",
        "Speed",
        "Type 2"
    ]
)
print(filtered)
# 即选出，满足条件的，其他列

"""
Observe that the group Water of column Type 1 
    is not contiguous相邻的. 
There are two rows of Grass in between. 
Also note that each pokemon within a group are sorted 
    by Speed in ascending order. 
Unfortunately, for this example we want them sorted 
    in descending speed order. 
Luckily with window functions this is easy to accomplish.

按Type 1分组
对"Name", "Speed"在组内降序排序
"""
out = filtered.with_columns(
    [
        pl.col(["Name", "Speed"])
        .sort(reverse=True)
        .over("Type 1"),
    ]
)
print(out)

"""
The power of window expressions is that 
    you often don't need a groupby -> explode combination, 
    but you can put the logic in a single expression. 
    It also makes the API cleaner. 
If properly used a:
    groupby -> marks that 
        groups are aggregated 
        and we expect a DataFrame of size n_groups
    over -> marks that 
        we want to compute something within a group,
        but doesn't modify the original size of the DataFrame

所以哪个快？

"""
#%% Window expression rules

"""
The evaluations of window expressions are as follows 
    (assuming we apply it to a pl.Int32 column):

"""
# aggregate and broadcast within a group
# output type: -> Int32
pl.sum("foo").over("groups")
# 分组求和

# sum within a group and multiply with group elements
# output type: -> Int32
( pl.col("x").sum() * pl.col("y") ).over("groups")
# 组内，求和并相乘

# sum within a group and multiply with group elements 
# and aggregate the group to a list
# output type: -> List(Int32)
( pl.col("x").sum() * pl.col("y") ).list().over("groups")
# 组内，求和并相乘
# 以list输出结果


# note that it will require an explicit `list()` call
# sum within a group and multiply with group elements 
# and aggregate the group to a list
# the flatten call explodes that list

# This is the fastest method to do things over groups 
# when the groups are sorted
( pl.col("x").sum() * pl.col("y") ).list().over("groups").flatten()


# flatten
"""
Explode a list or utf8 Series. 
This means that every item is expanded to a new row.
"""
df1 = pl.DataFrame({"foo": ["hello", "world"]})
df1
(df.select(pl.col("foo").flatten()))
# 即将hello 和 world全部展开胃一个个字母了


#%% More examples
"""
For more exercise, below are some window functions for
 us to compute:

1 sort all pokemon by type
对所有的宝梦可精灵按type排序
select the first 3 pokemon per type as "Type 1"
sort the pokemon within a type by speed and select the first 3 as "fastest/group"
sort the pokemon within a type by attack and select the first 3 as "strongest/group"
sort the pokemon by name within a type and select the first 3 as "sorted_by_alphabet"


"""
# 1 sort all pokemon by type
out = df.sort("Type 1").select(
    [
        pl.col("Type 1").head(3)
        .list().over("Type 1").flatten(),
        # 2 select the first 3 pokemon per type as "Type 1"
        # 相当于每个Type 1选择前3个
        # 注意，其中的flatten只是把list展开了
    
        
        pl.col("Name").sort_by(
            pl.col("Speed")
        ).head(3).list().over("Type 1").flatten().alias("fastest/group"),
        
        pl.col("Name").sort_by(
            pl.col("Attack")
        ).head(3).list().over("Type 1")
        .flatten().alias("strongest/group"),
        
        pl.col("Name").sort().head(3).list()
        .over("Type 1").flatten().alias("sorted_by_alphabet"),
    ]
)
out


# 解释下上面的flatten
out1 = df.sort("Type 1").select(
    [
        pl.col("Type 1").head(3)
        .list().over("Type 1"),
        # 2 select the first 3 pokemon per type as "Type 1"
        # 相当于每个Type 1选择前3个
        # 注意，其中的flatten只是把list展开
    ]
)
out1
# ["Bug", "Bug", "Bug"]  
# 所以才用的flatten
# 为什么不取消list呢？
# 如下的代码。会报错
# =============================================================================
# out2 = df.sort("Type 1").select(
#     [
#         pl.col("Type 1").head(3)
#         .over("Type 1"),
#         # 2 select the first 3 pokemon per type as "Type 1"
#         # 相当于每个Type 1选择前3个
#         # 注意，其中的flatten只是把list展开
#     ]
# )
# out2
# =============================================================================
# ComputeError: The length of the window expression did not match that of the group.
# 原因是我取的长度是3，与每个组的长度并不一致
#%% Flattened window function



