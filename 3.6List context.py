# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 23:25:59 2022

@author: xuanQS
"""
import polars as pl


#%% Row wise computations
grades = pl.DataFrame(
    {
        "student": ["bas", "laura", "tim", "jenny"],
        "arithmetic": [10, 5, 6, 8],
        "biology": [4, 6, 2, 7],
        "geography": [8, 4, 9, 7],
    }
)
print(grades)

out = grades.select([pl.concat_list(pl.all().exclude("student")).alias("all_grades")])
print(out)

#%% Running polars expression on list elements
# the percentage rank expression
rank_pct = pl.element().rank(reverse=True) / pl.col("").count()


grades.with_column(
    # create the list of homogeneous data
    pl.concat_list(pl.all().exclude("student")).alias("all_grades")
).select([
    # select all columns except the intermediate list
    pl.all().exclude("all_grades"),
    # compute the rank by calling `arr.eval`
    pl.col("all_grades").arr.eval(rank_pct, parallel=True).alias("grades_rank")
])

    
    

