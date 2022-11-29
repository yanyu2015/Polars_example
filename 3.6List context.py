# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:25:59 2022

@author: xuanQS
"""
import polars as pl

"""
An expression context we haven't discussed yet is 
    the List context. 
This means simply we can apply any expression on 
    the elements of a List

"""
#%% Row wise computations
"""
This context is ideal for computing things in row orientation.
对行方向的计算内容是理想的
Polars expressions work on columns that 
    have the guarantee that they consist of homogeneous data.
polars的表达式在有相同数据类型保障的类上工作
——即，每一列的各个元素的类型要是一致的
Columns have this guarantee, rows in a DataFrame not so much.
——但是显然，列的元素应该相同，而一行的元素类型并不一定相同
——因为不同列的类型可不同
Luckily we have a data type that 
    has the guarantee that the rows are homogeneous: 
        pl.List data type.
Let's say we have the following data:
"""
grades = pl.DataFrame(
    {
        "student": ["bas", "laura", "tim", "jenny"],
        "arithmetic": [10, 5, 6, 8],
        "biology": [4, 6, 2, 7],
        "geography": [8, 4, 9, 7],
    }
)
print(grades)
"""
If we want to compute the rank of all the columns 
    except for "student", 
we can collect those into a list data type:
This would give:
"""
out = grades.select(
    [
    pl.concat_list(
        pl.all().exclude("student")
    )
    .alias("all_grades")
    ]
 )
print(out)
# 对比，直接使用list函数,这是把一列压为一行的list
out1 = grades.select(
    [
    pl.all().exclude("student")
    .list()
    ]
 )
print(out1)

#%% Running polars expression on list elements
"""
We can run any polars expression on 
    the elements of a list with
    the arr.eval expression!
要在list的元素上运行表达式，要使用arr.eval表达式

These expressions run entirely on 
    polars' query engine and
    can run in parallel so will be super fast.
Let's expand the example from above with 
    something a little more interesting. 
Pandas allows you to compute the percentages of the rank values. 
Polars doesn't provide such a keyword argument. 
But because expressions are so versatile 
    we can create our own percentage rank expression.
    Let's try that!

Note that we must select the list's element
     from the context. 
——要选出list中的element，所以要用pl.element()
When we apply expressions over list elements, 
    we use pl.element() to select the element of a list.


"""
# the percentage rank expression
rank_pct = pl.element().rank(reverse=True) / pl.col("").count()
# 自动对每个元素调用rank函数
# pl.col("").count()是计数
# 那么中间的""表示什么呢?
# 输出的是rank百分比


grades.with_column(
    # create the list of homogeneous data
    pl.concat_list(
        pl.all().exclude("student")
    )
    .alias("all_grades")
).select([
    # select all columns except the intermediate list
    pl.all().exclude("all_grades"),
    
    # compute the rank by calling `arr.eval`
    pl.col("all_grades")
    .arr.eval( #要在list的元素上运行表达式，要使用arr.eval表达式
        rank_pct, 
        parallel=True #并行
    )
    .alias("grades_rank")
])

# 拆分看，就能看懂了
grades.with_column(
    # create the list of homogeneous data
    pl.concat_list(
        pl.all().exclude("student")
    )
    .alias("all_grades")
)

    

