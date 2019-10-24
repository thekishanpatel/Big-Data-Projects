from mrjob.job import MRJob
import mrjob
import math
import sys

class KMeans (MRJob):
    def distv (self, v1, v2):
        return math.sqrt(((v2[0] - v1[0])**2) + ((v2[1] - v1[1])**2))

    def configure_args(self):
        super(KMeans, self).configure_args()
        self.add_file_arg('--c')


    def gcentroids (self):
        f = open(r"DIRECTORY TO YOUR CENTROID FILE",'r')
        l = f.readline()
        cent = []
        while(l != ''):
            a = l.split(',')
            l2 = []
            for j in range(0, len(a), 1):
                l2.append(float(a[j]))
            cent.append(l2)
            l = f.readline()
        f.close()
        return cent

    def mapper(self, _, lines):
        cent = self.gcentroids()
        for l in lines.split('\n'):
            x,y = l.split(', ')
            p = [float(x), float(y)]
            mindist = sys.maxint
            classe = 0
            for i in range(len(cent)):
                dist = self.distv(p, cent[i])
                if dist < mindist:
                    mindist = dist
                    classe = i
        yield classe, p

    def reducer(self, k, v):
        c = 0
        mox = moy = 0.0
        for t in v:
            c += 1
            mox += t[0]
            moy += t[1]
        yield str(k), str((mox/c, moy/c))

if __name__ == '__main__':
    f = open(r"C:\Users\kpvp2\Desktop\KMeans\centroids.txt",'r')
    l = f.readline()
    cent = []
    while(l != ''):
        a = l.split(',')
        l2 = []
        for j in range(0, len(a), 1):
            l2.append(float(a[j]))
        cent.append(l2)
        l = f.readline()
    f.close()
    print("Original Centroids")
    for i in range(0, len(cent), 1):
        print("{} \t ({})".format(i,cent[i]))
    KMeans.run()
