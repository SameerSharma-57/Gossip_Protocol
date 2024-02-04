import subprocess
import platform

def run_in_new_terminal(command):
    if platform.system() == "Windows":
        subprocess.Popen(["start", "cmd", "/k", command], shell=True)
    elif platform.system() == "Linux":
        subprocess.Popen(["x-terminal-emulator", "-e", command])
    elif platform.system() == "Darwin":  # macOS
        subprocess.Popen(["open", "-a", "Terminal", command])

def start_servers():
    with open("config.csv", 'r') as f:
        lines = f.readlines()[1:]
        for line in lines:
            ip, port, node_id = line.strip().split(',')
            command = f"python echo-server.py {port} {node_id}"
            print(f"Starting server for port {port} and node_id {node_id}")
            run_in_new_terminal(command)

if __name__ == "__main__":
    start_servers()
