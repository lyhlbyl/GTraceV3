import numpy as np
import pandas as pd

'''
Table design:


'''

def hello(arg1, arg2):
    print(f"abc rocks: {arg1, arg2}")


if __name__ == '__main__':
    hello("hello", "world")
    x = np.array([[1,2],[3,4], [1, 0]])
    w = np.sum(x, axis=0) / np.count_nonzero(x, axis=0)
    print(w)



