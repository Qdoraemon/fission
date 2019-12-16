import os
import threading
import random
import string
import zipfile
import timeit
import math


SOURCE_PAHT = os.path.join(os.getcwd(), "source_file")
FILE_PATH = os.path.join(os.getcwd(), "files")
# DEFAULT_SPLIT_CAPACITY = "100m"
DEFAULT_SUFFIX = "wewe_"
ZIP_FILES = os.path.join(os.getcwd(), "zip_files")
# 得到最大的文件 单位：G
SPLIT_FILE_SIZE = 0.1
# 每个生成文件的大小
SMALL_FIEL_SIZE = 10
# 每块文件分多少份
SOLIT_SMALLE_FLIE_SIZE = 10
DEFAULT_SPLIT_CAPACITY = str(SOLIT_SMALLE_FLIE_SIZE) + "m"


def getrandSplitNum(file_name):
    num = getFileOrDirsSize(file_name) / SOLIT_SMALLE_FLIE_SIZE / 2

    return math.floor(num)


# 生成文件的操作
def createFile(file_path):
    while True:

        rand_int = random.randint(10, 1000)
        world = get_random_str(rand_int)
        rand_bool = random.randint(0, 1)
        with open(file_path, 'a') as f:
            f.write(world + " ")
            if rand_bool == 1:
                f.write("\n")
        if getFileOrDirsSize(file_path) > SMALL_FIEL_SIZE:
            return


# 获取文件或者文件夹的大小
def getFileOrDirsSize(path, type="M"):
    file_or_dir_size = 0
    if os.path.isfile(path):
        file_or_dir_size = os.path.getsize(path)
    else:
        for root, _, files in os.walk(path):
            file_or_dir_size += sum([os.path.getsize(os.path.join(root, name)) for name in files])
    if type == "M":
        file_or_dir_size = file_or_dir_size / float(1024*1024)
    elif type == "G":
        file_or_dir_size = file_or_dir_size / float(1024*1024*1024)
    return round(file_or_dir_size, 2)


# 获取随机字符串
def get_random_str(size=10):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


# 开始多线程生成项目
def startThreads(num=10):
    threads = []
    for i in range(10):

        # path = os.path.join(os.getcwd(), "files", "source_"+str(i)+".txt")
        path = os.path.join(FILE_PATH, "source_"+str(i)+".txt")
        t = threading.Thread(target=createFile, args=(path, ))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    return


# 打包
def zip_file_dir(path, zip_file_name):
    filelist = []
    if os.path.isfile(path, ):
        filelist.append(path)
    else:
        for root, dirs, files in os.walk(path):
            for name in files:
                filelist.append(os.path.join(root, name))

    zip_file = zipfile.ZipFile(zip_file_name, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        zip_file.write(tar)
    zip_file.close()


# 分割包
def splitFile(name, capacity=DEFAULT_SPLIT_CAPACITY, suffix=DEFAULT_SUFFIX):
    os.chdir(SOURCE_PAHT)
    # path = os.path.join(SOURCE_PAHT, name)
    os.popen("split -a 2 -b {} {} {}".format(capacity, name, suffix))
    return True


# 获取分割包的名称
def getSplitFileList(suffix=DEFAULT_SUFFIX):
    cmd = os.popen("ls -1 {} |grep {}".format(SOURCE_PAHT, suffix))
    files_str = cmd.read()
    file_list = files_str.strip("\n").split("\n")
    return file_list


def zipFiles(file_list):
    file_num = random.randint(1, 999999)
    zip_file_name = os.path.join(ZIP_FILES, "{}.zip".format(file_num))

    zip_file = zipfile.ZipFile(zip_file_name, "w", zipfile.zlib.DEFLATED)
    for tar in file_list:
        tar_file = os.path.join(SOURCE_PAHT, tar)
        zip_file.write(tar_file)
    zip_file.close()


# 组合新的结构
def createBigDirs(file_num):
    file_list = getSplitFileList()
    choice_list = []
    max_file_num = random.randint(1, file_num)
    while True:
        choice_list.append(random.choice(file_list))
        if len(choice_list) >= max_file_num:
            zipFiles(choice_list)
            choice_list = []
            max_file_num = random.randint(1, 5)
        if getFileOrDirsSize(ZIP_FILES, "G") >= SPLIT_FILE_SIZE:
            break


# 流程 如果自己准备了 exist_file 请放到source_file 目录下面 
def mainController(exist_file="", ):
    if len(exist_file) <= 0:
        startThreads()
        file_name = random.randint(1, 1000000)
        exist_file = os.path.join(SOURCE_PAHT, "{}.zip".format(str(file_name)))
        zip_file_dir(FILE_PATH, exist_file)
    else:
        exist_file = os.path.join(SOURCE_PAHT, exist_file)
    # 分割
    splitFile(exist_file)
    threads = []
    num = getrandSplitNum(exist_file)
    for i in range(10):
        t = threading.Thread(target=createBigDirs, args=(num, ))
        threads.append(t)
        t.start()
    for t in threads:
        print("sdsd")
        t.join()


if __name__ == "__main__":
    start = timeit.default_timer()
    # 这里可以添加 exist_file 文件。。 请放到source_file，只需要文件名称就可以了。。 
    mainController("631200.zip")

    end = timeit.default_timer()
    print(str(end - start))
    # 完成只有数据会在 zip_files 下面
