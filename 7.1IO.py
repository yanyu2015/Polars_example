# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 21:35:59 2022

@author: xuanQS

这个我只分析CSV和parquet
因为日常用这两个多

"""

import polars as pl

#%% CSV
# 读
df = pl.read_csv("path.csv")
# 写
df = pl.DataFrame({"foo": [1, 2, 3], "bar": [None, "bak", "baz"]})
df.write_csv("path.csv")


# lazy模式
df = pl.scan_csv("./dataset/iris.csv")
type(df)
# 即文档中说的LazyFrame
df.head()
# 此时，是不能显示数据的
df.head().collect()
# 此时，才会显示数据


#%% parquet
# 读
df = pl.read_parquet("path.parquet")
# 尽管文档提到
# Pandas uses PyArrow -Python bindings exposed by Arrow- to load Parquet files into memory
# 而polars是直接就读取，无需转换
# 实际上，经过一年的迭代，polars的这个函数才完善
# 之前使用的时候，他不支持设置列的格式
# 所以对于时间格式和id编码之类的有点麻烦

# 写
df = pl.DataFrame({"foo": [1, 2, 3], "bar": [None, "bak", "baz"]})
df.write_parquet("path.parquet")
# lazy模式
df = pl.scan_parquet("path.parquet")








