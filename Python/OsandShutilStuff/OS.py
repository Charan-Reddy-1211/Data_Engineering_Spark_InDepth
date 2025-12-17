import os

#To get the current working directory
print("Current working directory",os.getcwd())

#List files in directory
files_list=os.listdir(os.getcwd())
print("Files List: ",files_list)

#Check if it is file or directory
for file in files_list:
    if file.endswith(".py"):
        print("Python file: ",file)
    else:
        print("Directory: ",file)
print()

for file in files_list:
    if os.path.isdir(file):
        print("Directory: ",file)
    elif os.path.isfile(file):
        print("File: ",file)
print()

#os.path.join

print(os.path.join(os.getcwd(),"sales","sample.csv"))

#Check if path exists

print(os.path.exists(os.path.join(os.getcwd(),"sales","sample.csv")))

#Get the absolute path -> Need to check
print(os.path.abspath("Project_1"))

#Get base name and directory name
print(os.path.basename(os.getcwd()))
print(os.path.dirname(r"E:\Data Engineering\Python\Data_Engineering_Spark_InDepth\Python\OsandShutilStuff\sales\sample.csv"))

#Environment
env=os.environ.get("")
print(env)

#Downloaded
new_path=r"D:\Downloads\Charan_Reddy_Resume.docx"

# files=os.listdir(new_path)
#
# for file in files:
#     # print(file)
#     full_path=os.path.join(new_path,file)
#     if os.path.isdir(full_path)==True:
#         list_files=os.listdir(full_path)
#         for f in list_files:
#             print("Loop files: ",f)
#         # print("Directory: ",file)
#     elif os.path.isfile(full_path):
#         print("File: ",file)

info=os.stat(new_path)
print(info.st_size/1024/1024)