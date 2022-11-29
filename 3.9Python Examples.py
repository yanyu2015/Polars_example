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

#%%  Selection context，选择内容
# We can select by name
(df.select([
    pl.col("A"),
    # 表示A列
    
    "B",      
    # the col part is inferred
    # 自动推断
    # 表示B列
    
    pl.lit("B"),  
    # we must tell polars we mean the literal "B"
    # 表示维度相同的字面量B
    
    pl.col("fruits"),
]))

# you can select columns with a regex 
# if it starts with '^' and ends with '$'
# 使用通配符，表示从A列到B列结束
(df.select([
    pl.col("^A|B$").sum()
]))


# you can select multiple columns by name
# 选取多列
(df.select([
    pl.col(["A", "B"]).sum()
]))

# We select everything in normal order
# Then we select everything in reversed order
# 选择所有列
(df.select([
    pl.all(),
    pl.all().reverse().suffix("_reverse")
]))

# all expressions run in parallel
# single valued `Series` are broadcasted to the shape of the `DataFrame`
# 维度被扩展
(df.select([
    pl.all(),
    pl.all().sum().suffix("_sum")
]))

# there are `str` and `dt` namespaces for
# specialized functions
# 选出fruits列中包含字符b的行
predicate = pl.col("fruits").str.contains("^b.*")
(df.select([
    predicate
]))
# 这里给出的是bool类型
# 再通过filter选行
# use the predicate to filter
df.filter(predicate)
# 跟R的tidyverse的设计好像啊
# 都是通过filter选择行，select选择列

# predicate expressions can be used to filter
(df.select([
    pl.col("A")
    .filter(
        pl.col("fruits").str.contains("^b.*")
    ).sum(),
    # 对满足条件的A进行求和
    
    (pl.col("B").filter(pl.col("cars").str.contains("^b.*")).sum() * pl.col("B").sum()).alias("some_compute()"),
]))


# We can do arithmetic on columns and (literal) values
# can be evaluated to 1 without programmer knowing

some_var = 1
(df.select([
    ((pl.col("A") / 124.0 * pl.col("B")) / pl.sum("B") * some_var)
    .alias("computed")
]))


# We can combine columns by a predicate
# 如果满足条件，则对其复制
# 其实就是pandas中的按条件修正值
(df.select([
    "fruits",
    "B",
    pl.when(pl.col("fruits") == "banana")
    .then(pl.col("B")).otherwise(-1).alias("b")
]))


# We can combine columns by a fold operation on column level

(df.select([
    "A",
    "B",
    pl.fold(0, 
            lambda a, b: a + b, 
            [pl.col("A"), "B", pl.col("B")**2, pl.col("A") / 2.0]
    ).alias("fold")
]))
# pl.fold就是对
# 后面【】中的四列，比如这四列的第一行为：
# 1，5,25,0.5
# 他们相加就是31.5

# even combine all
(df.select([
    pl.arange(0, df.height).alias("idx"),
    # 其中df.height是df的高，即行数
    # 相当于生成了一个从0到len的索引序列
    
    "A",
    pl.col("A").shift().alias("A_shifted"),
    # A列的滞后列
    
    pl.concat_str(pl.all(), "-").alias("str_concat_1"),  
    # prefer this
    # 所有列的内容，通过-分隔符，连接起来
    
    pl.fold(pl.col("A"), 
            lambda a, b: a + "-" + b, pl.all().exclude("A")
    ).alias("str_concat_2"),  
    # over this (accidentally O(n^2))
    # 通过fold的方式连接
    # 算法复杂度更高，效率低
    
]))


#%% Aggregation context
# expressions are applied over groups instead of columns
# 即作用到每个组上，而不是每个列

# we can still combine many expressions
(df.sort("cars").groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        # 对组内的B求和
        
        pl.sum("B").alias("B_sum2"),  
        # syntactic sugar for the first
        # 语法糖使得其与上面的代码效果一致
        
        pl.first("fruits").alias("fruits_first"),
        # fruit列的第一列
        
        pl.count("A").alias("count"),
        pl.col("cars").reverse()
    ]))


# We can explode the list column "cars"
# 先看这个
(df.sort("cars").groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        
        pl.sum("B").alias("B_sum2"),  
        # syntactic sugar for the first
        
        pl.first("fruits").alias("fruits_first"),
        
        pl.count("A").alias("count"),
        
        pl.col("cars").reverse()
    ]))
"""
shape: (2, 6)
也就是，对每个组进行运算
"""
df
# 把结果扩展到与cars一个维度
(df.sort("cars").groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        
        pl.sum("B").alias("B_sum2"),  
        # syntactic sugar for the first
        
        pl.first("fruits").alias("fruits_first"),
        
        pl.count("A").alias("count"),
        
        pl.col("cars").reverse()
    ])).explode("cars")
# 每次结果不同，其实不如加上maintain

# 按中间生成的部分数据
(df.groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        pl.sum("B").alias("B_sum2"),  
        # syntactic sugar for the first
        
        pl.first("fruits").alias("fruits_first"),
        pl.count(),
        pl.col("B").shift().alias("B_shifted")
    ])
 .explode("B_shifted")
)
# 对比，不扩展的话，多个维度的结果，就被压缩成list了
(df.groupby("fruits")
    .agg([
        pl.col("B").sum().alias("B_sum"),
        pl.sum("B").alias("B_sum2"),  
        # syntactic sugar for the first
        
        pl.first("fruits").alias("fruits_first"),
        pl.count(),
        pl.col("B").shift().alias("B_shifted")
    ])
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
# 作为对比
(df.sort("cars").groupby("fruits")
    .agg([
        pl.col("B").sum(),
        pl.sum("B").alias("B_sum2"),  # syntactic sugar for the first
        pl.first("fruits").alias("fruits_first"),
        pl.count("A").alias("count"),
        pl.col("cars").reverse()
    ]))
# 可见，此时是list
# 不过之前有些结果不是会扩展么？
# 为啥这里没？
# 因为这里是agg
# 不是select吧
# 而agg在pandas中也是压缩的
# 只有对应的transform才会扩展


# we can also get the list of the groups
# 分组agg后得到list
(df.groupby("fruits")
    .agg([
         pl.col("B").shift().alias("shift_B"),
         pl.col("B").reverse().alias("rev_B"),
    ]))

# we can do predicates in the groupby as well
# 分组agg
# 但是结果只显示满足条件的那些B列的值
(df.groupby("fruits")
    .agg([
        pl.col("B")
        .filter(pl.col("B") > 1).list().keep_name(),
    ]))


# and sum only by the values 
# where the predicates are true
# 分组agg
# 但是结果只显示满足条件的那些B列的值的均值
(df.groupby("fruits")
    .agg([
        pl.col("B").filter(pl.col("B") > 1).mean(),
    ]))

# Another example
# 移动后，即滞后出现的缺失值，用0来填补
(df.groupby("fruits")
    .agg([
        pl.col("B")
        .shift_and_fill(1, fill_value=0)
        .alias("shifted"),
        
        pl.col("B").shift_and_fill(1, fill_value=0).sum().alias("shifted_sum"),
    ]))



#%% Window functions!
# pl.col("foo").aggregation_expression(..).over("column_used_to_group")
# 其实over在可以可以理解为分组的作用
# groupby 2 different columns
(df.select([
    "fruits",
    "cars",
    "B",
    pl.col("B").sum()
        .over("fruits").alias("B_sum_by_fruits"),
    pl.col("B").sum()
        .over("cars").alias("B_sum_by_cars"),
]))

# reverse B by groups and show the results 
# in original DF
(df.select([
    "fruits",
    "B",
    pl.col("B").reverse()
    .over("fruits").alias("B_reversed_by_fruits")
]))

# Lag a column within "fruits"
# 求滞后期
(df.select([
    "fruits",
    "B",
    pl.col("B").shift()
    .over("fruits").alias("lag_B_by_fruits")
]))

