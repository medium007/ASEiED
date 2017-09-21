import json

dataIn = json.load(open("data.json"))

dataOut = open("dataOut.json", 'w')

for data in dataIn['data']:
    dataOut.write('{')
    #dataOut.write('\n')
    for i in range(len(dataIn['cols'])):
        #dataOut.write('\t')

        if isinstance(data[i], int):
            dataOut.write('"' + dataIn['cols'][i].encode('utf-8') + '": "' + str(data[i]) + '"')
        else:
            dataOut.write('"' + dataIn['cols'][i].encode('utf-8') + '": "' + data[i].encode('utf-8') + '"')
        if i != len(dataIn['cols']) - 1:
            dataOut.write(', ')
        #dataOut.write('\n')

    dataOut.write('}')
    dataOut.write('\n')
