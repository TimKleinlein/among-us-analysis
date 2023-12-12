import os
import sys

sessions = os.listdir("../../../dimstore/pop520978/data")
print(sessions)

with open('output_file1.txt', 'w') as file1:
    sys.stdout = file1
    print(f"Output for file 1: {sessions[0]}")

with open('output_file2.txt', 'w') as file2:
    sys.stdout = file2
    print(f"Output for file 2: {sessions[1]}")
