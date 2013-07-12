#!/usr/bin/python
import matplotlib.pyplot as plt
#Open the Corresponding File
################ File for nodes v/s time in embedded graph ################
fo = open("../src/storm/main/Outputs/create-nodes-time-embedded.dat","r")
################ File for nodes v/s time in server mode ####################
f1 = open("../src/storm/main/Outputs/create-nodes-time-server.dat","r")
#Read the tuples and create the list of co-ordinates
X = []
Y = []
X1 = []
Y1 = []

check = True
while(check):
    line = fo.readline()
    line1 = f1.readline()
    
    temp = line.split(",")
    temp1 = line1.split(",")
    
    a = long(temp[0])
    b = long(temp[-1][:-1])
    X.append(a)
    Y.append(b)
    
    a1 = long(temp1[0])
    b1 = long(temp1[-1][:-1])
    X1.append(a1)
    Y1.append(b1)
    print a,b,a1,b1
    if (a==1000):
        check = False

plt.title("Nodes v/s time in Server and Embedded")
plt.xlabel("Nodes")
plt.ylabel("Time")
line1 = plt.plot(X,Y,label="Embedded")
line2 = plt.plot(X1,Y1,label="Server")
plt.setp(line2,color='r',linewidth='1.0')
plt.legend(loc="upper left")
plt.show()