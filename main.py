# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import json
import pathlib

BASE_DIR = pathlib.Path(__file__).parent.resolve()
FILES_DIR = f"{BASE_DIR}/files"

python_data = None


def print_file():

    with open(f"{FILES_DIR}/json_data.json", "r") as file_stream:
        python_data = json.load(file_stream)

        print(f'Deserialized data type: {type(python_data)}')
        print(f'Deserialized data: {python_data}')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_file()

