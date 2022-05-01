from attr import field
import numpy as np
import sys
import os 
import random
import time
import matplotlib.pyplot as plt
import math
import csv
arr = [[0,0,0,0]]

# b = [[0],[0],[0]]
# arr = np.concatenate([arr, b])
def check_arr_size():
    gb_const = 1073741824
    # a =  os.path.getsize('D:\\test1\\out.csv', )
    a = sys.getsizeof(arr)
    file_size = a / gb_const
    # print(file_size)
    return file_size

def generate_datachunk():
    heigh = 1080
    width = 1900
    radius = (heigh + width) / 4 
    b = []
    anc_x = random.randint(0, width)
    anc_y = random.randint(0, heigh)
    anc_r = random.randint(1, 40)
    while len(b) < 100:
        anc_l = random.randint(0,359)
        r = random.randint(1, anc_r)
        y = r*math.sin(anc_l)
        x = r*math.cos(anc_l)
        b.append([x + anc_x,y + anc_y, 1, time.time()+random.randint(0,20000)])

    return b
def save_to_file(a:np.array):
    f = open("./out.csv", "a", newline="")
    w = csv.writer(f,delimiter = ";")
    w.writerows(a.tolist())
def test(size):
    global arr
    while check_arr_size() <= 3.1:
    # while len(arr) < size*1000:
        
        b = generate_datachunk()
        # arr = np.concatenate([arr, b])
        # arr = arr.astype(int)
        # b.astype(int)
        a = len(arr)
        a = np.array(b)
        a = a.astype(int)
        check_arr_size()
        save_to_file(a)
        # save_to_file(arr)
        # np.savetxt('out.csv', arr, delimiter=";")
        b = 0
    x = []
    y = []
    for i in arr:
        x.append(i[0])
        y.append(i[1])
    # plt.scatter(x,y)
    # plt.show()
        
    m = 0


test(5)