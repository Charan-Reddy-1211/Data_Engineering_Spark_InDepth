number=6

#Divisible by 1 and that number, so count should be 2
count=0
for i in range(1,number+1):
    if number%i==0:
        count=count+1

if count==2:
    print(f"Given number {number} is prime number")
else:
    print(f"Given number {number} is NOT prime number")