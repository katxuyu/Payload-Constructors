import json
import gzip
import os

import sys

fname = sys.argv[1]

dir_from = f"{fname}"
dir_to = f"/data/main"


for file_name in os.listdir(dir_from): 
    f = os.path.join(dir_from, file_name)
    t = os.path.join(dir_to, file_name)
    if os.path.isfile(f) and file_name:
        fn = file_name.split(".")[0] 
        folder_name = str(fn).replace(f"-{fn.split('-')[-3]}-{fn.split('-')[-2]}-{fn.split('-')[-1]}", "")
        dir_to_c = t = os.path.join(dir_to, folder_name)
        dir_to_d = os.path.join(dir_to_c, file_name)
        if not os.path.isdir(dir_to_c):
            os.makedirs(dir_to_c)
        if not os.path.isfile(dir_to_d) and file_name:
            os.system(f"sudo cp {f} {dir_to_d}")
        else:
            q = open(f"{dir_to}/error.txt", "a")
            q.write(f"{f}\n")
            q.close()
    else:
            q = open(f"{dir_to}/error.txt", "a")
            q.write(f"{f} --> Not a file\n")
            q.close()

