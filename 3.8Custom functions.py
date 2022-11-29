# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:26:41 2022

@author: xuanQS
"""
import polars as pl


"""
Still, you need to have the power to be able to 
    pass an expression's state to a third party library 
    or apply your black box function over data in polars.
内置的表达式已经有强大的功能
但是仍然需要使用第三方包和自定义的函数
For this we provide the following expressions:
    map
    apply
"""
#%% To map or to apply.
"""
These functions have an important distinction in
     how they operate and
     consequently what data they will pass to the user.

A map passes the Series backed by the expression as is.
map函数返回的是一个Series

map follows the same rules in both the select and
    the groupby context, this will mean that 
    the Series represents a column in a DataFrame. 
    这意味着Series表示DataFrame中的一列
Note that in the groupby context, 
    that column is not yet aggregated!

Use cases for map are for instance passing the Series 
    in an expression to a third party library. 
    Below we show how we could use map to 
    pass an expression column to a neural network model.
    
df.with_column([
    pl.col("features")
    .map(lambda s: MyNeuralNetwork.forward(s.to_numpy()))
    .alias("activations")
])
"""


"""
Use cases for map in the groupby context are slim. 
They are only used for performance reasons, 
but can quite easily lead to incorrect results. 
Let me explain why.
groupby中map的使用情况应很少。
它们仅用于性能原因，但很容易导致错误的结果
"""



df = pl.DataFrame(
    {
        "keys": ["a", "a", "b"],
        "values": [10, 7, 1],
    }
)
df
out = df.groupby("keys", maintain_order=True).agg(
    [
        pl.col("values")
        .map(lambda s: s.shift())
        .alias("shift_map"),
        
        pl.col("values")
        .shift()
        .alias("shift_expression"),
    ]
)
print(out)
"""
In the snippet above we groupby the "keys" column. 
That means we have the following groups:
即有如下的两个
    "a" -> [10, 7]
    "b" -> [1]
apply a shift operation to the right
即如果我们要向右移动，即得到的是上一期，那么结果如下
    "a" -> [null, 10]
    "b" -> [null]
但是显然out的结果是错的

This went horribly wrong, 
because the map applies the function before we aggregate!
 So that means the whole column [10, 7, 1] 
    got shifted to [null, 10, 7] and was then aggregated.
map运行在agg之前(我感觉是在分组之前吧)
也就是说，map把[10, 7, 1] 变成[null, 10, 7]
再被分组，切成了
[null, 10]和[7]

So my advice is to never use map in the groupby context unless you know you need it and know what you are doing.
也就是不要在groupby中使用map
"""



#%% To apply
"""
Luckily we can fix previous example with apply. 
apply works on the smallest logical elements for that operation.

That is:
    select context -> single elements
    groupby context -> single groups
So with apply we should be able to fix our example:
"""
out = df.groupby("keys", maintain_order=True).agg(
    [
        pl.col("values")
        .apply(lambda s: s.shift())
        .alias("shift_map"),
        
        pl.col("values")
        .shift()
        .alias("shift_expression"),
    ]
)
print(out)


#%% apply in the select context
"""
In the select context, 
the apply expression passes elements of the column 
to the python function.
即apply是应用到列的每一个元素
Note that you are now running python, this will be slow.

Let's go through some examples to see what to expect. 
We will continue with the DataFrame we defined 
at the start of this section and
 show an example with the apply function and
 a counter example where
 we use the expression API to achieve the same goals.
两个例子，一个是使用apply函数
一个是使用the expression API
"""
#%%% Adding a counter
"""
In this example we create a global counter and 
then add the integer 1 to the global state 
    at every element processed. 
Every iteration the result of the increment will 
    be added to the element value.


"""
counter = 0
# 约束输入和返回参数的类型，会快一些
# 其实这里conuter与在函数内部，设定为1，是不同的
# 但是呢，这样每次调用完就被回收，效率比较低，也不能每次都保存数据
# 所以就定义为全局变量
def add_counter(val: int) -> int:
    global counter # 这个跟julia太像了
    counter += 1
    return counter + val
df
out = df.select(
    [
        pl.col("values")
        .apply(add_counter)
        .alias("solution_apply"),
        # 对列中的每个元素加1
        
        (pl.col("values") + 
         pl.arange(1, pl.count() + 1))
        .alias("solution_expr"),
    ]
)
print(out)

# 要理解pl.count()可以看如下
df.select(
    [
         pl.arange(1, pl.count() + 1),
    ]
)
# 其实pl.count()就是3
df.select(
    [
         pl.count(),
    ]
)
# 就是Count the number of values in this column/context.
# 即列中的元素数


#%%% Combining multiple column values
"""
If we want to have access to values of 
    different columns in a single apply function call, 
    we can create struct data type. 
This data type collects those columns as fields in the struct.
So if we'd create a struct from the columns "keys" and "values", 
    we would get the following struct elements:
    [
        {"keys": "a", "values": 10},
        {"keys": "a", "values": 7},
        {"keys": "b", "values": 1},
    ]
注意，意思就是我们如何要对不同列的值使用同一个apply函数
就要使用pl.struct(["keys", "values"])
其创建的数据结构是struct
结构体的概念在julia，C++中都有
其实可以当做是第三方包提供的一种数据类型
就像pandas提供了DataFrame一样
至于上面的【】中一大串不用去管它。
反正我们可以通过操作dict的方式，在python中进行get、set
"""


out = df.select(
    [
        pl.struct(["keys", "values"])
        .apply(lambda x: len(x["keys"]) + x["values"]) 
        .alias("solution_apply"),
        # 第一列keys的长度，加上第二列values的值
        
        (pl.col("keys").str.lengths() 
         + pl.col("values")).alias("solution_expr"),
    ]
)
print(out)
"""
In Python, 
those would be passed as dict to the calling python function and
 can thus be indexed by field: str


"""

#%% Return types?
"""
由于polars并不知道自定义函数做了什么
而他又是自动推导类型的
所以他是根据第一个非空值的类型来判断的
The data type is automatically inferred. 
We do that by waiting for the first non-null value. 
That value will then be used to determine the type of the Series.

The mapping of python types to polars data types is as follows:
    int -> Int64
    float -> Float64
    bool -> Boolean
    str -> Utf8
    list[tp] -> List[tp] (where the inner type is inferred with the same rules)
    dict[str, [tp]] -> struct
    Any -> object (Prevent this at all times)

"""


