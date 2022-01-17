import os
import csv
import time
import datetime
import fileinput


DataFilePath = "datasetGenerator/dataset.csv"

BDFilePath = "BD/"
HeapPath = BDFilePath + "HeapBD.txt"
HeapHeadPath = BDFilePath + "HeapHEAD.txt"
OrderedPath = BDFilePath + "OrderedBD.txt"
OrderedHeadPath = BDFilePath + "OrderedHEAD.txt"
HashPath = BDFilePath + "HashBD.txt"
HashHeadPath = BDFilePath + "HashHEAD.txt"

paddingCharacter = "#"

recordSize = 97+1  # 97 chars + escape key

blockSize = 5

heapHeadSize = 5

orderedHeadSize = 5

hashHeadSize = 5

bucketSize = 10

numberOfBuckets = 10

dicColunaTamanhoMax = {
    "A": 6,   # DOC_NUMBER
    "B": 60,  # NOME
    "C": 10,  # DATA
    "D": 11,  # CPF, PK
    "E": 10,  # SALARIO
}

dicColHeaderType = {
    "CPF":       "INTEGER(11)",
    'DOCNUMBER': "INTEGER(6)",
    'NOME':      "VARCHAR(60)",
    "DATA":      "DATE",
    'SALARIO':   "VARCHAR(10)",
}

maxColSizesList = [11, 6, 60, 10, 10]

colHeadersList = ["CPF", "DOCNUMBER", "NOME", "DATA", "SALARIO"]


relevantColsList = [0, 1, 2, 3, 4]


def IsRelevantRow(rowNumber):
    return rowNumber in relevantColsList


def CalculateRecordSize():
    sum = 0
    for key, value in dicColunaTamanhoMax:
        sum += value
    return sum


def FillCPF(cpf):
    return cpf.zfill(maxColSizesList[0])  # tamanho de CPF e fixo


def PadString(stringToPad, totalSizeOfField):
    tmp = stringToPad
    for i in range(totalSizeOfField - len(stringToPad)):
        tmp += paddingCharacter
    return tmp


def ReadFirstRecordsFromCSV(CSVFilePath, numberOfRecordsToRead):
    valuesToLoad = PadRecords(ReadFromFile(CSVFilePath))

    for i in range(numberOfRecordsToRead):
        string = ""
        for j in range(len(maxColSizesList)):
            string += valuesToLoad[i][j]
        valuesToLoad[i] = string
    return valuesToLoad[:numberOfRecordsToRead]


def ReadFirstRecordsFromCSVList(CSVFilePath, numberOfRecordsToRead):
    valuesToLoad = PadRecords(ReadFromFile(CSVFilePath))
    return valuesToLoad[:numberOfRecordsToRead]


def ReadFromFile(csvFilePath):
    lineCount = 0
    registros = []
    with open(csvFilePath, 'r') as file:
        rows = csv.reader(file, delimiter=",")
        for row in rows:
            if lineCount == 0:  # headers
                lineCount += 1
            else:
                finalRow = []
                for i in range(len(row)):
                    if IsRelevantRow(i):
                        if i == relevantColsList[3]:
                            finalRow.insert(0, FillCPF(row[i]))
                        else:
                            finalRow += [(row[i])]
                if finalRow[0] == "":
                    return registros  # chegou numa linha vazia, fim do arquivo
                registros += [finalRow]
                lineCount += 1
    return registros


def PadRecords(listOfRecords):
    for i in range(len(listOfRecords)):
        for j in range(len(listOfRecords[i])):
            listOfRecords[i][j] = PadString(
                listOfRecords[i][j], maxColSizesList[j])
    return listOfRecords


def CleanRecord(recordString):
    newRecord = []
    offset = 0
    for i in range(len(maxColSizesList)):
        newRecord += [recordString[offset:offset+maxColSizesList[i]
                                   ].replace(paddingCharacter, "").replace("\n", "")]

        offset += maxColSizesList[i]
    return newRecord


def InsertLineIntoFile(record, location, filepath):
    for line in fileinput.input(filepath, inplace=1):
        linenum = fileinput.lineno()
        if linenum == location:
            line = line + record
        else:
            line = line.rstrip()
        print(line)


def DeleteLineFromFile(location, filepath):
    for line in fileinput.input(filepath, inplace=1):
        linenum = fileinput.lineno()
        if linenum == location+1:
            continue
        else:
            line = line.rstrip()
            print(line)


def MakeHEADString(headType, numRecords):
    string = "File structure: " + headType + "\n"
    string += "Creation: " + \
        datetime.datetime.fromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + \
        datetime.datetime.fromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "\nNumber of records: " + str(numRecords) + "\n"

    return string


def MakeHEAD(headPath, headType, numRecords):
    if os.path.exists(headPath):
        os.remove(headPath)
    file = open(headPath, 'a')
    string = "File structure: " + headType + "\n"
    string += "Creation: " + \
        datetime.datetime.fromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + \
        datetime.datetime.fromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "\nNumber of records: " + str(numRecords) + "\n"
    file.write(string)


def UpdateHEADFile(headPath, headType, numRecords):
    if os.path.exists(headPath):
        file = open(headPath, 'r')

        headContent = file.readlines()
        headContent
        file.close()
        os.remove(headPath)

        file = open(headPath, 'a')
        file.write(headContent[0])
        file.write(headContent[1])
        file.write("Last modification: " + datetime.datetime.fromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n")
        file.write(headContent[3])
        file.write("Number of records: " + str(numRecords) + "\n")
    else:
        MakeHEAD(headPath, headType, numRecords)


def GetNumRecords(DBHeadFilePath, headSize):
    with open(DBHeadFilePath, 'r') as file:
        for i in range(headSize):  # para bd ordenado
            file.readline()
        return (int(file.readline().split("Number of records: ")[1]))


def FetchBlock(DBFilePath, startingRecord):
    block = []
    with open(DBFilePath, 'r') as file:

        for i in range(recordSize*startingRecord):
            c = file.read(1)

        for i in range(blockSize):
            record = ""
            for j in range(recordSize):
                c = file.read(1)
                if c == "":
                    return block
                record += c
            block += [CleanRecord(record)]
    return block


def FetchBlock2(DBFilePath, startingRecord, recordCustomSize):
    block = []
    with open(DBFilePath, 'r') as file:
        for i in range(recordCustomSize*startingRecord):
            c = file.read(1)

        for i in range(blockSize):
            record = ""
            for j in range(recordCustomSize):
                c = file.read(1)
                if c == "":
                    return block
                record += c
            block += [CleanRecord(record)]
    return block
