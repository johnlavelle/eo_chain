import os

def clean_file(filename):

    input_file = open(filename,'r')

    data = []

    for line in input_file:

        if 'TILT' in line :

            try:

                x = line.split('TILT;')[1]

            except:
                print('blank line')
                continue

            x = x.split(';')

            processed_line = 'TILT;'

            for e in x :

                if '$' in e :
                    continue

                else:
                    processed_line += e + ';'

            line = processed_line.rstrip(';')


        data.append(line)

    input_file = open(filename,'w')

    for element in data:

        input_file.write(element)

def process_files():

    for x in os.listdir('/data/CoastVAL/Mat'+'/input'):

        if '.csv' in x :

            clean_file('input/'+x)


        


