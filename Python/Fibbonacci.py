number=5

a=0
b=1

#Sample for number 5 is 0,1,1,2,3,5
#                       a,b,c
#                         a,b
print(a)
print(b)
for i in range(1,number+1):
    c=a+b
    a=b
    b=c
    print(c)
