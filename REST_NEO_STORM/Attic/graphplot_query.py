#!/usr/bin/python
### Script to generate tuples for plotting of the time tuples v/s time and tuples v/s memory
import matplotlib.pyplot as plt
#Open the Corresponding File

################ File for query-tuples v/s time ############################
fo = open("../src/storm/main/Outputs/query-tuples-memory.dat","r")
################ File for query-tuples v/s time ############################
f1 = open("../src/storm/main/Outputs/query-tuples-memory.dat","r")
################ File for query-tuples v/s time ##########################
f2 = open("../src/storm/main/Outputs/query-tuples-time-30bolts20spouts.dat","r")
################ File for query-tuples v/s time ##########################
f3 = open("../src/storm/main/Outputs/query-tuples-time-40bolts20spouts.dat","r")
################ File for query-tuples v/s time ##########################
f4 = open("../src/storm/main/Outputs/query-tuples-time-50bolts20spouts.dat","r")

#Read the tuples and create the list of co-ordinates
X=[]
Y=[]
X1=[]
Y1=[]
X2=[]
Y2=[]
X3=[]
Y3=[]
X4=[]
Y4=[]
count = 0
check = True
while(count!=48):
    line = fo.readline()
    line1 = f1.readline()
    line2 = f2.readline()
    line3 = f3.readline()
    line4 = f4.readline()
    
    temp = line.split(",")
    temp1 = line1.split(",")
    temp2 = line2.split(",")
    temp3 = line3.split(",")
    temp4 = line4.split(",")
    
    a = long(temp[0])
    b = float(temp[-1][:-1])
    X.append(a)
    Y.append(b)
    
    #a1 = long(temp1[0])
    #b1 = long(temp1[-1][:-1])
    #X1.append(a1)
    #Y1.append(b1)
    
    #a2 = long(temp2[0])
    #b2 = long(temp2[-1][:-1])
    #X2.append(a2)
    #Y2.append(b2)
    
    #a3 = long(temp3[0])
    #b3 = long(temp3[-1][:-1])
    #X3.append(a3)
    #Y3.append(b3)
    
    #a4 = long(temp4[0])
    #b4 = long(temp4[-1][:-1])
    #X4.append(a4)
    #Y4.append(b4)
    
    #print a,b,a1,b1,a2,b2,a3,b3,a4,b4
    count=count+1

plt.title("Queries v/s Memory(total)")
#plt.title("Queries v/s Memory")
plt.xlabel("Queries")
plt.ylabel("Memory (Mb)")
#plt.ylabel("Memory")
line1 = plt.plot(X,Y,label="10 Bolt 20 Spout")
#line2 = plt.plot(X1,Y1,label="20 Bolts 20 Spouts")
#line3 = plt.plot(X2,Y2,label="30 Bolts 20 Spouts")
#line4 = plt.plot(X3,Y3,label="40 Bolts, 20 Spouts")
#line5 = plt.plot(X4,Y4,label="50 Bolts, 20 Spouts")
#plt.setp(line2,color='r',linewidth='1.0')
#plt.setp(line3,color='g',linewidth='1.0')
#plt.setp(line4,color='y',linewidth='1.0')
#plt.setp(line5,color='m',linewidth='1.0')
plt.legend(loc="upper left")
plt.show()
#plt.savefig("graph.png")