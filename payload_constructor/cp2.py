import json
import gzip
import os

import sys

fname = sys.argv[1]

# dir_from = f"{fname}"
dir_to = f"/data/main"

reps = []
with open("repeat.txt") as infile:
    reps = infile.read().split('\n')

for r in reps:
    file_name = r.split("/")[-1]
    dir_from = r.replace(f"{file_name}", "")
    f = os.path.join(dir_from, file_name)
    t = os.path.join(dir_to, file_name)
    if os.path.isfile(f) and file_name:
        fn = file_name.split(".")[0] 
        folder_name = str(fn).replace(f"-{fn.split('-')[-3]}-{fn.split('-')[-2]}-{fn.split('-')[-1]}", "")
        dir_to_c = t = os.path.join(dir_to, folder_name)
        dir_to_d = os.path.join(dir_to_c, file_name)
        if not os.path.isdir(dir_to_c):
            os.makedirs(dir_to_c)
        if not os.path.isfile(dir_to_d):
            os.system(f"sudo cp {f} {dir_to_d}")
        else:
            #print(dir_to_d, f)
            os.system(f"sudo chmod 777 {dir_to_d}; sudo gunzip {dir_to_d}; sudo zcat {f} >> {dir_to_d.replace('.gz', '')}; sudo gzip -9 {dir_to_d.replace('.gz', '')}")
            #pass
    else:
            q = open(f"{dir_to}/error.txt", "a")
            q.write(f"{f} --> Not a file\n")
            q.close()

