import names
import random
import time
import sys
from random_date_generator import *
from cpf_generator import cpf
from io import StringIO
import os

DATASET_SIZE_IN_MB = 5  # Change this var to fit your needs

DATASET_OUTPUT_DIRECTORY = "./dataset.csv"

RANDOM_DATE_START = "01/01/1990"
RANDOM_DATE_END = "01/01/2022"

MAX_SALARY = 5000000
MIN_SALARY = 950

MAX_MB_PER_BATCH = 50


def GenerateName():
    """
    Generates a name using the names lib: https://pypi.org/project/names/
    Effectively Returns a full name in string form.
    """
    return names.get_full_name()


def GenerateRandomDate():
    """Generate a random date between RANDOM_DATE_START AND RANDOM_DATE_END to be used as birthday."""
    return random_date(RANDOM_DATE_START, RANDOM_DATE_END, random.random())


def GenerateCPF():
    return cpf()


def GenerateSalary():
    return round(MIN_SALARY + random.random() * (MAX_SALARY - MIN_SALARY), 2)


def GenerateRecord(id):
    return f"{str(id)},{GenerateName()},{GenerateRandomDate()},{GenerateCPF()},{GenerateSalary()}"


def GenerateString(max_bytes):
    return


id = 1


def GenerateDatasetPortion():
    """Generates around MAX_MBYTES_PER_BATCH data"""
    global id
    string_io = StringIO()
    while (sys.getsizeof(string_io.getvalue())/(1024*2) < MAX_MB_PER_BATCH):

        string_io.write(GenerateRecord(id) + "\n")
        id += 1
    return string_io.getvalue()


def getDatasetSizeInMB():
    return os.path.getsize(DATASET_OUTPUT_DIRECTORY)/(1024*2)


def GenerateDatasetHeader():
    return 'DOCNUMBER,NOME,DATA,CPF,SALARIO\n'


def GenerateDataset():
    with open(DATASET_OUTPUT_DIRECTORY, "a") as dataset_file:
        dataset_file.write(GenerateDatasetHeader())
        start_size_in_MB = getDatasetSizeInMB()
        print(f"Initial dataset size is {start_size_in_MB}MB")

        while getDatasetSizeInMB() < DATASET_SIZE_IN_MB:
            print(
                f"We are at size {getDatasetSizeInMB()}MB of {DATASET_SIZE_IN_MB}MB")
            portion = GenerateDatasetPortion()
            dataset_file.write(portion)
    print(f"Final dataset size was {getDatasetSizeInMB()}MB")


GenerateDataset()
