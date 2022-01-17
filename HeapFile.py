import os
import Functions as bd


def CreateHeapBD(csvFilePath):

    values_to_load = bd.ReadFromFile(csvFilePath)
    valuesToLoad = bd.PadRecords(values_to_load)

    if os.path.exists(bd.HeapPath):
        os.remove(bd.HeapPath)

    bd.MakeHEAD(bd.HeapHeadPath, "Heap", 0)

    recordCounter = 0
    print('1', values_to_load)
    print('2', valuesToLoad)
    for row in valuesToLoad:
        HeapInsertSingleRecord(row)
        recordCounter += 1

    bd.UpdateHEADFile(bd.HeapHeadPath, "HEAP", recordCounter)


def HeapSelectRecord(colName, value, singleRecordSelection=False, valueIsArray=False, secondColName="", secondValue=""):
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

    print("\nRunning query: ")
    if singleRecordSelection:
        if valueIsArray:
            print("\nSELECT * FROM TB_HEAP WHERE " +
                  colName + " in (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value +
                      " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " +
                      colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nSELECT * FROM TB_HEAP WHERE " +
                  colName + " in (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " +
                      value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " +
                      colName + " = " + value + ";\n\n")

    currentRecord = 0  # busca linear, sempre começamos do primeiro
    results = []
    while not (recordFound or endOfFile):
        currentBlock = bd.FetchBlock(bd.HeapPath, currentRecord)
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
        if valueIsArray:
            print("Não foi encontrado registro com " +
                  colName + " in (" + values + ")")
        else:
            print("Não foi encontrado registro com valor " +
                  colName + " = " + value)

    else:
        print("Results found: \n")
        for result in results:
            print(result)
            print("\n")

    print("End of search.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))


def HeapInsertSingleRecord(listOfValues):
    if len(listOfValues) != len(bd.maxColSizesList):
        print("Erro: lista de valores recebidos não tem a mesma quantidade de campos da relação")
        return
    with open(bd.HeapPath, 'a') as file:
        file.write(bd.FillCPF(listOfValues[0]))
        for i in range(1, len(listOfValues)):
            file.write(bd.PadString(listOfValues[i], bd.maxColSizesList[i]))
        file.write("\n")
    bd.UpdateHEADFile(bd.HeapHeadPath, "Heap", bd.GetNumRecords(
        bd.HeapHeadPath, bd.heapHeadSize)+1)


def HeapMassInsertCSV(csvFilePath):
    valuesToLoad = bd.PadRecords(bd.ReadFromFile(csvFilePath))

    recordCounter = bd.GetNumRecords(bd.HeapHeadPath, bd.heapHeadSize)
    for row in valuesToLoad:
        HeapInsertSingleRecord(row)
        recordCounter += 1

    bd.UpdateHEADFile(bd.HeapHeadPath, "HEAP", recordCounter)


def HeapDeleteRecord(colName, value, singleRecordDeletion=False, valueIsArray=False, secondColName="", secondValue=""):
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

    print("\nRunning query: ")
    if singleRecordDeletion:
        if valueIsArray:
            print("\nDELETE FROM TB_HEAP WHERE " + colName +
                  " in (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value +
                      " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nDELETE FROM TB_HEAP WHERE " +
                      colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nDELETE FROM TB_HEAP WHERE " +
                  colName + " in (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " +
                      value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nDELETE FROM TB_HEAP WHERE " +
                      colName + " = " + value + ";\n\n")

    currentRecord = 0  # busca linear, sempre começamos do primeiro
    results = []  # retornar os deletados
    while not (recordFound or endOfFile):
        currentBlock = bd.FetchBlock(bd.HeapPath, currentRecord)
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
                    bd.DeleteLineFromFile(currentRecord+i, bd.HeapPath)
                    recordFound = True
                    break
        currentRecord += bd.blockSize

    if results == []:
        if valueIsArray:
            print("Não foi encontrado registro com " +
                  colName + " in (" + values + ")")
        else:
            print("Não foi encontrado registro com valor " +
                  colName + " = " + value)

    else:
        print(indexesToDelete)

        for reg in reversed(indexesToDelete):
            bd.DeleteLineFromFile(reg, bd.HeapPath)
        print("\n\nRecords deleted: \n")
        for result in results:
            print(result)
            print("\n")

    print("End of query.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

    if results != []:
        bd.UpdateHEADFile(bd.HeapHeadPath, "Heap", bd.GetNumRecords(
            bd.HeapHeadPath, bd.heapHeadSize)-len(results))
