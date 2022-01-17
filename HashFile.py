import os
import Functions as bd


def CreateHashBD(csvFilePath):

    valuesToLoad = bd.PadRecords(bd.ReadFromFile(csvFilePath))

    if os.path.exists(bd.HashPath):
        os.remove(bd.HashPath)

    with open(bd.HashPath, 'wb') as hashFile:
        hashFile.seek((bd.bucketSize * bd.numberOfBuckets *
                      bd.blockSize * (bd.recordSize - 1)) - 1)
        hashFile.write(b'\0')

    bd.MakeHEAD(bd.HashHeadPath, "Hash", 0)

    recordCounter = 0
    for row in valuesToLoad:
        record = Record(row, False)
        HashInsertRecord(record)
        recordCounter += 1


def MassHashInsert(csvFilePath):
    valuesToLoad = bd.PadRecords(bd.ReadFromFile(csvFilePath))

    recordCounter = 0
    for row in valuesToLoad:
        record = Record(row, False)
        HashInsertRecord(record)
        recordCounter += 1


def CalculateHashKey(key):
    return int(key)


def CalculateHashAddress(hashKey):
    return hashKey % bd.numberOfBuckets


def FetchBlockBytes(hashFile, startOffset):
    hashFile.seek(startOffset)
    return hashFile.read((bd.blockSize * (bd.recordSize - 1)))


class Bucket:
    def __init__(self, hashFile, startOffset):
        self.blocksList = []
        for i in range(startOffset, startOffset + bd.bucketSize * bd.blockSize * (bd.recordSize - 1) - 1, bd.blockSize * (bd.recordSize - 1)):
            self.blocksList += [Block(FetchBlockBytes(hashFile, i))]
        self.firstBlockWithEmptyRecordIndex = self.__FirstBlockWithEmptyRecordIndex()

    def __FirstBlockWithEmptyRecordIndex(self):
        for i in range(len(self.blocksList)):
            if (self.blocksList[i].firstEmptyRecordIndex != -1):
                return i

        return -1


class Block:

    def __init__(self, recordsBytes):
        self.recordsList = []
        for b in range(0, len(recordsBytes), (bd.recordSize - 1)):
            self.recordsList += [Record(recordsBytes[b: b +
                                        (bd.recordSize - 1)], True)]

        self.firstEmptyRecordIndex = self.__FirstEmptyRecordIndex()

    def SizeInBytes(self):
        sizeInBytes = 0
        for record in self.recordsList:
            sizeInBytes += record.sizeInBytes

        return sizeInBytes

    def __FirstEmptyRecordIndex(self):
        for i in range(len(self.recordsList)):
            try:
                if (self.recordsList[i].docNumber.index('\x00') >= 0):
                    return i
            except:
                pass
        return -1

    def __str__(self):
        str_block = ""
        for record in self.recordsList:
            str_block += str(record)

        return str_block


class Record:

    def __init__(self, listOfValues, dataInBytes):
        if (not dataInBytes):
            self.docNumber = listOfValues[0]
            self.name = listOfValues[1]
            self.date = listOfValues[2]
            self.cpf = listOfValues[3]
            self.salary = listOfValues[4]
        else:
            listOfValues = listOfValues.decode("utf-8")
            self.docNumber = listOfValues[0:6]
            self.name = listOfValues[6:66]
            self.date = listOfValues[66:76]
            self.cpf = listOfValues[76:87]
            self.salary = listOfValues[87:97]

        self.sizeInBytes = len(str(self))

    def __str__(self):
        return self.docNumber + self.name + self.date + self.cpf + self.salary

    def Clear(self):
        self.docNumber = '\x00' * 6
        self.name = '\x00' * 60
        self.date = '\x00' * 10
        self.cpf = '\x00' * 11
        self.salary = '\x00' * 10

        self.sizeInBytes = len(str(self))


def HashInsertRecord(record):
    freeBlockIndex = -1
    freeSpaceIndex = -1

    hashKey = CalculateHashKey(record.docNumber)
    hashAddress = CalculateHashAddress(hashKey)

    startingOffset = hashAddress * bd.bucketSize * \
        bd.blockSize * (bd.recordSize - 1)

    with open(bd.HashPath, 'r+b') as hashFile:
        while freeBlockIndex == -1:
            currentBucket = Bucket(hashFile, startingOffset)
            freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
            if (freeBlockIndex == -1):
                startingOffset += bd.bucketSize * \
                    bd.blockSize * (bd.recordSize - 1)
                pass
            else:
                currentBlock = currentBucket.blocksList[freeBlockIndex]

                freeSpaceIndex = currentBlock.firstEmptyRecordIndex
                currentBlock.recordsList[freeSpaceIndex] = record

        hashFile.seek(startingOffset + (freeBlockIndex *
                      bd.blockSize * (bd.recordSize - 1)))
        hashFile.write(str(currentBlock).encode("utf-8"))


def HashDeleteRecord(searchKeys, goodSearchKeys):
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0
        hashKey = CalculateHashKey(searchKey)
        hashAddress = CalculateHashAddress(hashKey)

        startingOffset = hashAddress * bd.bucketSize * \
            bd.blockSize * (bd.recordSize - 1)

        with open(bd.HashPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRecord = False
                for i in range(len(currentBucket.blocksList)):
                    block = currentBucket.blocksList[i]
                    blocksVisitedCount += 1
                    for record in block.recordsList:
                        if (record.docNumber == searchKey):
                            record.Clear()
                            foundRecord = True
                            hashFile.seek(
                                startingOffset + (i * bd.blockSize * (bd.recordSize - 1)))
                            hashFile.write(str(block).encode("utf-8"))
                            print("Blocks visited for key {}: {}".format(
                                searchKey, blocksVisitedCount))
                            if (freeBlockIndex == -1):
                                freeBlockIndex = 0
                            break

                    if (foundRecord):
                        break

                if (not foundRecord):
                    if (freeBlockIndex == -1):
                        startingOffset += bd.bucketSize * \
                            bd.blockSize * (bd.recordSize - 1)
                        pass
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(
                            searchKey, blocksVisitedCount))
                        pass


def HashSelectRecord(searchKeys, goodSearchKeys):
    recordList = []
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0
        hashKey = CalculateHashKey(searchKey)
        hashAddress = CalculateHashAddress(hashKey)

        startingOffset = hashAddress * bd.bucketSize * \
            bd.blockSize * (bd.recordSize - 1)

        with open(bd.HashPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRecord = False
                for block in currentBucket.blocksList:
                    blocksVisitedCount += 1
                    for record in block.recordsList:
                        if (record.docNumber == searchKey):
                            recordList += [record]
                            foundRecord = True
                            print("Blocks visited for key {}: {}".format(
                                searchKey, blocksVisitedCount))
                            if (freeBlockIndex == -1):
                                freeBlockIndex = 0
                            break

                    if (foundRecord):
                        break

                if (not foundRecord):
                    if (freeBlockIndex == -1):
                        startingOffset += bd.bucketSize * \
                            bd.blockSize * (bd.recordSize - 1)
                        pass
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(
                            searchKey, blocksVisitedCount))
                        pass

    return recordList


def HashLinearSelectRecord(colName, value, customRecordSize, singleRecordSelection=False, valueIsArray=False, secondColName="", secondValue=""):
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
        currentBlock = bd.FetchBlock2(
            bd.HashPath, currentRecord, customRecordSize)
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
    return results
