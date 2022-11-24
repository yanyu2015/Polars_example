# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 12:02:55 2022

@author: xuanQS
"""

"""
设定文件所在路径为当前路径
"""
import os
# 当前文件的路径
print('current file path'+os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))
print(os.getcwd())