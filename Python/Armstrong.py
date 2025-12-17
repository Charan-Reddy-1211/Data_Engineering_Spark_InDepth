number=153

#Armstrong -> sum of cubes of each digit equals to same number
#convert to string because we need to split the digits
numbers=str(number)
sum=0
for num in numbers:
    sum=sum+int(num)*int(num)*int(num)
print("Final sum: ",sum)

if sum==number:
    print("Yes, the given number is Armstrong number")
else:
    print("No, the given number is not Armstrong number")