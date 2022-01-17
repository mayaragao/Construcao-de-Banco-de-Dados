import math
from dateutil import parser
import os
import fileinput
import Functions as bd


def LinearSelectRecord(colName, value, singleRecordSelection=False, valueIsArray=False, secondColName="", secondValue=""):
    numberOfBlocksUsed = 0  # conta o número de vezes que "acessamos a memória do disco"
    recordFound = False
    endOfFile = False

    values = ""
    if valueIsArray:
        for val in value:
            values += val + ", "
        values = values[:len(values)-2]  # tira ultima ', '

    if colName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = bd.colHeadersList.index(colName)

    secondValuePresent = False

    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in bd.colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = bd.colHeadersList.index(secondColName)
        secondValuePresent = True

    currentRecord = 0  # busca linear, sempre começamos do primeiro
    results = []
    while not (recordFound or endOfFile):
        currentBlock = bd.FetchBlock(bd.OrderedPath, currentRecord)
        if currentBlock == []:
            endOfFile = True
            break

        numberOfBlocksUsed += 1

        for i in range(len(currentBlock)):
            if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex] == value and currentBlock[i][secondColumnIndex] == secondValue))) or (valueIsArray and currentBlock[i][columnIndex] in value):
                print("Result found in record " + str(currentRecord+i) + "!")
                results += [currentBlock[i]]
                if singleRecordSelection:
                    recordFound = True
                    break
        currentRecord += bd.blockSize

    if results == []:
        return -1, numberOfBlocksUsed
    else:
        return results, numberOfBlocksUsed


def DeleteRecord(colName, value, singleRecordDeletion=False, valueIsArray=False, secondColName="", secondValue=""):
    numberOfBlocksUsed = 0  # conta o número de vezes que "acessamos a memória do disco"
    recordFound = False
    endOfFile = False

    indexesToDelete = []

    values = ""
    if valueIsArray:
        for val in value:
            values += val + ", "
        values = values[:len(values)-2]  # tira ultima ', '

    if colName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = bd.colHeadersList.index(colName)

    secondValuePresent = False

    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in bd.colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = bd.colHeadersList.index(secondColName)
        secondValuePresent = True

    currentRecord = 0  # busca linear, sempre começamos do primeiro
    results = []  # retornar os deletados
    while not (recordFound or endOfFile):
        currentBlock = bd.FetchBlock(bd.OrderedPath, currentRecord)
        if currentBlock == []:
            endOfFile = True
            break

        numberOfBlocksUsed += 1

        for i in range(len(currentBlock)):
            if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex] == value and currentBlock[i][secondColumnIndex] == secondValue))) or (valueIsArray and currentBlock[i][columnIndex] in value):
                print("Result found in record " + str(currentRecord+i) + "!")
                results += [currentBlock[i]]
                indexesToDelete += [currentRecord+i]

                if singleRecordDeletion:
                    bd.DeleteLineFromFile(currentRecord+i, bd.OrderedPath)
                    recordFound = True
                    break
        currentRecord += bd.blockSize

    if results == []:
        return -1, numberOfBlocksUsed

    else:
        for reg in reversed(indexesToDelete):
            bd.DeleteLineFromFile(reg, bd.OrderedPath)

    print("End of query.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

    if results != []:
        bd.UpdateHEADFile(bd.OrderedHeadPath, "Ordered", bd.GetNumRecords(
            bd.OrderedHeadPath, bd.heapHeadSize-1)-len(results))

    return results, numberOfBlocksUsed


numColToOrder = 1


def CreateOrderedBD(csvFilePath):
    valuesToLoad = bd.PadRecords(bd.ReadFromFile(csvFilePath))
    valuesToLoad = sortList(valuesToLoad)
    if os.path.exists(bd.OrderedPath):
        os.remove(bd.OrderedPath)

    bd.MakeHEAD(bd.OrderedHeadPath, "Ordered", len(valuesToLoad))

    file = open(bd.OrderedPath, "w+")
    for row in valuesToLoad:
        file.write(bd.FillCPF(row[0]))
        for i in range(1, len(row)):
            file.write(bd.PadString(row[i], bd.maxColSizesList[i]))
        file.write("\n")

    file.close()


def sortComparison(elem):
    if(numColToOrder == 3):
        return parser.parse(elem[numColToOrder])
    else:
        return elem[numColToOrder]


def sortList(values):
    return sorted(values, key=sortComparison)


def binarySearch(columnIndex, value, maxNumBlocks, singleRecordSelection=False):
    numberOfBlocksUsed = []

    foundedBlocks = []

    accessedBlocks = []

    l = 0
    r = maxNumBlocks
    while l <= r:
        mid = math.ceil(l + (r - l)/2)

        blockRecords = bd.FetchBlock(bd.OrderedPath, (mid-1)*5)

        getNearBlocks(mid, foundedBlocks, columnIndex, value, accessedBlocks,
                      maxNumBlocks, numberOfBlocksUsed, singleRecordSelection)
        if (foundedBlocks):
            return foundedBlocks, len(numberOfBlocksUsed)
        else:
            if(value > blockRecords[-1][columnIndex]):
                l = mid + 1
            else:
                r = mid - 1

    return -1, len(numberOfBlocksUsed)


def getNearBlocks(numberBlock, foundedBlocks, columnIndex, value, accessedBlocks,
                  maxNumBlocks, numberOfBlocksUsed, singleRecordSelection=False):

    if(numberBlock not in accessedBlocks):
        accessedBlocks.append(numberBlock)
        numberOfBlocksUsed.append('1')

        indexesFoundedBlocks = []

        blockRecords = bd.FetchBlock(bd.OrderedPath, (numberBlock-1)*5)

        for idx, block in enumerate(blockRecords):
            if value in block[columnIndex]:
                if(singleRecordSelection):
                    foundedBlocks.append(block)
                    return
                indexesFoundedBlocks.append(idx)
                foundedBlocks.append(block)

        if((0 in indexesFoundedBlocks) and numberBlock > 1):
            getNearBlocks(numberBlock-1, foundedBlocks,
                          columnIndex, value, accessedBlocks, maxNumBlocks, numberOfBlocksUsed)

        if((4 in indexesFoundedBlocks) and numberBlock < maxNumBlocks):
            getNearBlocks(numberBlock+1, foundedBlocks,
                          columnIndex, value, accessedBlocks, maxNumBlocks, numberOfBlocksUsed)


def OrderedSelectSingleRecord(colName, value, singleRecordSelection=True):
    numberOfBlocksUsed = 0  # conta o número de vezes que "acessamos a memória do disco"

    if colName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = bd.colHeadersList.index(colName)

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " +
          colName + " = " + value + " limit 1;")

    numBlocks = math.ceil(bd.GetNumRecords(
        bd.OrderedHeadPath, bd.heapHeadSize-1)/bd.blockSize)

    if(columnIndex == numColToOrder):
        blockFounded, numberOfBlocksUsed = binarySearch(columnIndex, value,
                                                        numBlocks, singleRecordSelection)
    else:
        blockFounded, numberOfBlocksUsed = LinearSelectRecord(
            colName, value, singleRecordSelection)  # fazer select normal

    if(blockFounded):
        print("Registro encontrado: ")
        print(blockFounded)
    else:
        print("Registro não encontrado")

    print("\nFim da busca.")
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))


def OrderedSelectWithMultipleValues(colName, values):
    numberOfBlocksUsed = 0  # conta o número de vezes que "acessamos a memória do disco"
    totalBlocksFounded = []
    totalNumberOfBlocksUsed = 0
    if colName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = bd.colHeadersList.index(colName)

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " +
          colName + " in " + str(values) + ";")

    numBlocks = math.ceil(bd.GetNumRecords(
        bd.OrderedHeadPath, bd.heapHeadSize-1)/bd.blockSize)

    if(columnIndex == numColToOrder):
        for value in values:
            blocksFounded, numberOfBlocksUsed = binarySearch(columnIndex, value,
                                                             numBlocks, singleRecordSelection=False)
            totalBlocksFounded.append(blocksFounded)
            totalNumberOfBlocksUsed += numberOfBlocksUsed
    else:
        blocksFounded, numberOfBlocksUsed = LinearSelectRecord(
            bd.OrderedPath, colName, values, valueIsArray=True)  # fazer select normal
        totalBlocksFounded.append(blocksFounded)
        totalNumberOfBlocksUsed += numberOfBlocksUsed

    if(len(totalBlocksFounded)):
        print("\nRegistro(s) encontrado(s): ")
        print(totalBlocksFounded)
    else:
        print("\nRegistro não encontrado")

    print("Fim da busca.")
    print("Número de blocos varridos: " + str(totalNumberOfBlocksUsed) + "\n")


def binarySearchWithTwoFields(columnIndex, value, maxNumBlocks,
                              secondColIndex="", secondValue=""):
    numberOfBlocksUsed = []

    foundedBlocks = []

    accessedBlocks = []

    l = 0
    r = maxNumBlocks
    while l <= r:
        mid = math.ceil(l + (r - l)/2)

        blockRecords = bd.FetchBlock(bd.OrderedPath, (mid-1)*5)

        getNearBlocksWithTwoFields(mid, foundedBlocks, columnIndex, value, accessedBlocks,
                                   maxNumBlocks, numberOfBlocksUsed,
                                   secondColIndex, secondValue)
        if (foundedBlocks):
            return foundedBlocks, len(numberOfBlocksUsed)
        else:
            if(value > blockRecords[-1][columnIndex]):
                l = mid + 1
            else:
                r = mid - 1

    return -1, len(numberOfBlocksUsed)


def getNearBlocksWithTwoFields(numberBlock, foundedBlocks, columnIndex, value, accessedBlocks,
                               maxNumBlocks, numberOfBlocksUsed, secondColIndex,
                               secondValue):

    if(numberBlock not in accessedBlocks):
        accessedBlocks.append(numberBlock)
        numberOfBlocksUsed.append('1')

        indexesFoundedBlocks = []

        blockRecords = bd.FetchBlock(bd.OrderedPath, (numberBlock-1)*5)

        for idx, block in enumerate(blockRecords):
            if value in block[columnIndex]:
                if(secondValue in block[secondColIndex]):
                    indexesFoundedBlocks.append(idx)
                    foundedBlocks.append(block)

        if((0 in indexesFoundedBlocks) and numberBlock > 1):
            getNearBlocksWithTwoFields(numberBlock-1, foundedBlocks,
                                       columnIndex, value, accessedBlocks, maxNumBlocks,
                                       numberOfBlocksUsed, secondColIndex, secondValue)

        if((4 in indexesFoundedBlocks) and numberBlock < maxNumBlocks):
            getNearBlocksWithTwoFields(numberBlock+1, foundedBlocks,
                                       columnIndex, value, accessedBlocks, maxNumBlocks,
                                       numberOfBlocksUsed, secondColIndex, secondValue)


def OrderedSelectWithTwoFields(colName, value, secondColName="", secondValue=""):
    numberOfBlocksUsed = 0  # conta o número de vezes que "acessamos a memória do disco"

    if colName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex1 = bd.colHeadersList.index(colName)

    if secondColName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex2 = bd.colHeadersList.index(secondColName)

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " = " + str(value) +
          " AND " + str(secondColName) + " = " + str(secondValue) + ";")

    numBlocks = math.ceil(bd.GetNumRecords(
        bd.OrderedHeadPath, bd.heapHeadSize-1)/bd.blockSize)

    if(columnIndex1 == numColToOrder):
        blocksFounded, numberOfBlocksUsed = binarySearchWithTwoFields(columnIndex1, value,
                                                                      numBlocks, columnIndex2, secondValue)
    elif(columnIndex2 == numColToOrder):
        blocksFounded, numberOfBlocksUsed = binarySearchWithTwoFields(columnIndex2, secondValue,
                                                                      numBlocks, columnIndex1, value)

    else:
        blocksFounded, numberOfBlocksUsed = LinearSelectRecord(
            colName, value, False, False, secondColName, secondValue)  # fazer select normal

    if(blocksFounded and blocksFounded != -1):
        print("Registro(s) encontrado(s): ")
        print(blocksFounded)
    else:
        print("Registro não encontrado")

    print("Fim da busca.")
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))


def binarySearchBetween(columnIndex, firstValue, maxNumBlocks, secondValue=""):
    numberOfBlocksUsed = []

    foundedBlocks = []

    accessedBlocks = []

    l = 0
    r = maxNumBlocks
    while l <= r:
        mid = math.ceil(l + (r - l)/2)

        blockRecords = bd.FetchBlock(bd.OrderedPath, (mid-1)*5)

        getNearBlocksBetween(mid, foundedBlocks, columnIndex, firstValue, accessedBlocks,
                             maxNumBlocks, numberOfBlocksUsed,
                             secondValue)
        if (foundedBlocks):
            return foundedBlocks, len(numberOfBlocksUsed)
        else:
            if(firstValue > blockRecords[-1][columnIndex]):
                l = mid + 1
            elif (secondValue < blockRecords[0][columnIndex]):
                r = mid - 1
            else:
                break

    return -1, len(numberOfBlocksUsed)


def getNearBlocksBetween(numberBlock, foundedBlocks, columnIndex, value, accessedBlocks,
                         maxNumBlocks, numberOfBlocksUsed, secondValue):

    if(numberBlock not in accessedBlocks):
        accessedBlocks.append(numberBlock)
        numberOfBlocksUsed.append('1')

        indexesFoundedBlocks = []

        blockRecords = bd.FetchBlock(bd.OrderedPath, (numberBlock-1)*5)

        for idx, block in enumerate(blockRecords):
            if block[columnIndex] >= value:
                if(block[columnIndex]) <= secondValue:
                    indexesFoundedBlocks.append(idx)
                    foundedBlocks.append(block)

        if((0 in indexesFoundedBlocks) and numberBlock > 1):
            getNearBlocksBetween(numberBlock-1, foundedBlocks,
                                 columnIndex, value, accessedBlocks, maxNumBlocks, numberOfBlocksUsed,
                                 secondValue)

        if((4 in indexesFoundedBlocks) and numberBlock < maxNumBlocks):
            getNearBlocksBetween(numberBlock+1, foundedBlocks,
                                 columnIndex, value, accessedBlocks, maxNumBlocks, numberOfBlocksUsed,
                                 secondValue)


def OrderedSelectBetweenTwoValues(colName, firstValue, secondValue):
    numberOfBlocksUsed = 0  # conta o número de vezes que "acessamos a memória do disco"

    if colName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex1 = bd.colHeadersList.index(colName)

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " BETWEEN " + str(firstValue) +
          " AND " + str(secondValue) + ";")

    numBlocks = math.ceil(bd.GetNumRecords(
        bd.OrderedHeadPath, bd.heapHeadSize-1)/bd.blockSize)

    if(columnIndex1 == numColToOrder):
        blocksFounded, numberOfBlocksUsed = binarySearchBetween(columnIndex1, firstValue,
                                                                numBlocks, secondValue)

    else:
        blocksFounded = None

    if(blocksFounded and blocksFounded != -1):
        print("Registro(s) encontrado(s): ")
        print(blocksFounded)
    else:
        print("Registro não encontrado")

    print("Fim da busca.")
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))


def InsertLineIntoFile(record, location, filepath):
    for line in fileinput.input(filepath, inplace=1):
        linenum = fileinput.lineno()
        if linenum == location:
            line = line + record
        else:
            line = line.rstrip()
        print(line)


def DeleteLineFromFile(record, filepath):
    for line in fileinput.FileInput(filepath, inplace=1):
        recordLine = [bd.CleanRecord(line)]
        if record == recordLine:
            continue
        else:
            line = line.rstrip()
            print(line)


def OrderdDeleteSingleRecord(colName, value, singleRecordSelection=True):
    if colName not in bd.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = bd.colHeadersList.index(colName)

    print("Running query: ")
    print("Delete * FROM TB_ORDERED WHERE " +
          colName + " = " + str(value) + " limit 1;")

    numBlocks = math.ceil(bd.GetNumRecords(
        bd.OrderedHeadPath, bd.heapHeadSize-1)/bd.blockSize)

    if(columnIndex == numColToOrder):
        blockFounded, numberOfBlocksUsed = binarySearch(columnIndex, value,
                                                        numBlocks, singleRecordSelection)
    else:
        blockFounded, numberOfBlocksUsed = DeleteRecord(
            colName, value, singleRecordSelection)  # fazer select normal

    if(blockFounded):
        print("Registro encontrado: ")
        print(blockFounded)
        DeleteLineFromFile(blockFounded, bd.OrderedPath)
        print("Registro Deletado")

    else:
        print("Registro não encontrado")

    print("Número de blocos varridos: " + str(numberOfBlocksUsed))
