

file_path="C:\\Users\\lakes\\Downloads\\lakesh.txt"

try:
    l=[]
    file = open(file_path, 'r')
    data=file.readlines()
    for line in data:
        for word in line:
            if word not in l:
                k=line.count(word)
                l.append(word)
                print(word,k)
                print("lakesh")


except:
    print("cant handle the file, may be the location invalid")

finally:
    print("program finished")