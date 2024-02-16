import subprocess
import platform
from time import sleep
import random

def run_in_new_terminal(command):
    if platform.system() == "Windows":
        subprocess.Popen(["start", "cmd", "/k", command], shell=True)
    elif platform.system() == "Linux":
        subprocess.Popen(["x-terminal-emulator", "-e", command])
    elif platform.system() == "Darwin":  # macOS
        subprocess.Popen(["open", "-a", "Terminal", command])

def start_servers():
    for i in range(5):
        command = f"python echo-client.py {i}"
        run_in_new_terminal(command)
        sleep(6)

if __name__ == "__main__":
    start_servers()
