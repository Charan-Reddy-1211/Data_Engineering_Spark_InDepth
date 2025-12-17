import os
import shutil

#Copy the file shutil.copy(src,dest)

#need to provide the full path
# src=r"E:\Docker\SQLServerImage\Commands.txt"
# print("Changing current working directory")
# os.chdir(r"D:\Data Engineering")
# shutil.copy(src,os.getcwd())
# print(f"Copying files from {src} to ",os.getcwd())

#Moves the file from one folder to another folder

# dest=r"D:\Data Engineering"
# dest_path=os.path.join(dest,"command.txt")
# shutil.move(dest_path,src)

#Copy folder

src=r"D:\Data Engineering\PySpark"
dest=r"D:\Data Engineering\New_Pyspark"

shutil.copytree(src,dest,dirs_exist_ok=True)