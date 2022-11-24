
#%% Quick start
import polars as pl

# df = pl.read_csv("https://j.mp/iriscsv")
# df.write_csv("./dataset/iris.csv")
df = pl.read_csv("./dataset/iris.csv")
# 从df中选出sepal_length中满足大于5的行
# 按species分组
# 并对每个组的所有行应用求和函数
print(df.filter(pl.col("sepal_length") > 5)
      .groupby("species")
      .agg(pl.all().sum())
)
# polars的输出会显示列名，且会显示列的类型
# 这个是学了R中的data.table的方式
# 这种换行的写法，参考的是pandas的写法


#%% Lazy quick start
print(
    df
    .lazy()
    .filter(pl.col("sepal_length") > 5)
    .groupby("species")
    .agg(pl.all().sum())
    .collect()
)
"""
上面多了一个lazy函数的调用
以及最后多了一个collect，收集的工作
When the data is stored locally, 
we can also use scan_csv in Python, 
or LazyCsvReader in Rust to run the query in lazy polars.
也就说是，现在我是已经读进来了
如果连读取也要用lazy 模式，就要用scan_csv或LazyCsvReader
"""

#%% Lazy quick start

"""
The lazy API builds a query plan. 
Nothing is executed 
    until you explicitly ask Polars to execute the query
        (via LazyFrame.collect(),
         or LazyFrame.fetch()). 
This provides Polars with the entire context of the query, 
    allowing optimizations and choosing the fastest algorithm given that context.
Going from eager to lazy is often as simple as 
    starting your query with .lazy() and ending with .collect().
    
即，polars有两种工作模式，即eager，和lazy
其中eager就是立刻执行
对于lazy模式而言，lazy模式给查询一个全流程的内容（抽象的概念）
那么polars就可据此内容最优化选择最快的算法
从eager → lazy，只有在最前面加上lazy，最后面加上collect
但是不是说collect或fetch会执行么？
是的，其实就是之前几步合并成一步，然后用了最优的算法，才执行
而之前的eager，则是每一步执行完了再进行下一步。
    # step1 从df中选出sepal_length中满足大于5的行
    # step2 按species分组
    # step3 并对每个组的所有行应用求和函数
举个例子：
    假如我需要从南门去格致楼
    我代码写的步骤是先从南门到图书馆猛男雕像处，再走银杏道去格致楼
    即分成2步，两个直角边
    
    但如果我用lazy模式呢，就是等代码写的步骤写完了，还是跟上面的一样
    但是polars做了一些隐藏的代码优化，他发现其实可以直接从南门走斜边到格致楼，就更快了，
    当然，polars未必有这么智能，但数据操作的很多部分，其实可以互相共享，
    他可能有一些隐藏的机制可以优化，这也是最优化的问题
    这个机制，在dask中，其实也是一样的

"""
(
    df.lazy()
    .filter(pl.col("sepal_length") > 5)
    .groupby("species")
    .agg(pl.all().sum())
    .collect()
)

