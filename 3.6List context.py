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

