import csv

fp = open(r"C:\Users\wangjing\Desktop\meinianshouyi.txt")
alldata = fp.read().split("\n")
for i in range(len(alldata)):
    try:
        data_ = alldata[i].split(",")
        name_ = data_[0]
        time_ = data_[1]
        rat_ = data_[2]
        xiapu_ = data_[3]
    except:
        data_ = ''
        name_ = ''
        time_ = ''
        rat_ = ''
        xiapu_ = ''
    if data_ == '' or name_ == '' or time_ == '' or rat_ == '' or xiapu_ == '':
        continue

    print(name_,time_)
    fp = open(r"C:\Users\wangjing\Desktop\jielun666.txt")
    alldata = fp.read().split("\n")
    for i in range(len(alldata)):
        try:
            data = alldata[i].split("\t")
            name = data[0]
            rat = data[1]
            xiapu = data[2]
            suanfa = data[3]
            jielun = data[4]
        except:
            data = ''
            name = ''
            rat = ''
            xiapu = ''
            suanfa = ''
            jielun = ''
        if data == '' or name == '' or rat == '' or xiapu == '' or suanfa == '' or jielun == '':
            continue
# fp = open(r"C:\Users\wangjing\Desktop\jielun666.txt")
# alldata = fp.read().split("\n")
# for i in range(len(alldata)):
#     data = alldata[i].split("\t")
#     name = data[0]
#     rat = data[1]
#     xiapu = data[2]
#     suanfa = data[3]
#     jielun = data[4]
#     if name_ == name:
        #print(name, rat)