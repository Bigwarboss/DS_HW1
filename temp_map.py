import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    filepath = 'res.txt'
    marked = dict()
    with open(filepath, "r") as fd:
        lines = fd.readlines()
        for each in lines:
            splited = each.split('\t')
            name = splited[0]
            value = splited[1]
            marked[name] = int(value)
        a = np.zeros((1080//120, 1920//120))
        for key in marked.keys():
            print(key)
            a[int(key[4:]) // 16, int(key[4:]) % 16] = marked[key]

    plt.imshow(a, cmap='hot', interpolation='nearest')
    plt.savefig('output.png')
    plt.show()
