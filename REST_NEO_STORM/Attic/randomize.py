from random import randint

#fo = open("update.dat","r")
#f1 = open("update1.dat","w")

# generate random weights for update.dat
#tuple = fo.readline()
#temp = tuple.split(" ")
#while(temp[0]=='c'):
#	new_tuple = temp[0]+" "+temp[1]+" "+temp[2]+" "+str(randint(1,99))
#	f1.write(new_tuple+"\n")
#	tuple = fo.readline()
#	temp = tuple.split(" ")

#generate random weights for query

f2 = open("query.dat","w")
count = 1
while count!=300:
	new_tuple = "q"+" "+str(randint(1,1000))+" "+str(randint(1,1000))
	f2.write(new_tuple+"\n")
	count=count+1