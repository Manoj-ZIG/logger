
import builtins
import csv
import os
from datetime import datetime

LOG_FILE = "/Users/manojkumar.nagula/Downloads/custom_losgs.csv"


# Ensure CSV file has headersx
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Message"])

def custom_print(*args, **kwargs):
    timestamp = datetime.now().isoformat(timespec='milliseconds')
    message = " ".join(str(arg) for arg in args)
    formatted_message = f"{timestamp}   {message}"

    # Print the formatted message to console
    builtins._original_print(formatted_message, **kwargs)

    # Log to CSV if ":-" is present
    if ":-" in message:
        with open(LOG_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, message])

def enable_custom_logging():
    builtins._original_print = builtins.print
    builtins.print = custom_print

def disable_custom_logging():
    builtins.print = builtins._original_print
