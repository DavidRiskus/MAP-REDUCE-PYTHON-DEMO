from mrjob.job import MRJob
import csv

""" This program will take a CSV data file and determines the maximum of all numbers in the file """


            #all values are mapped under the same key "Max number"
            #"Max number", 2
            #"Max number", 2
            #"Max number", 3
            #"Max number", 4
            #"Max number", 3

            #Grouper groups under the key "Max number" all numbers in every line
            #Result "Max number", [2,2,3,4,3]

            #Reducer receives exactly that "Max number", [2,2,3,4,3]

            #Parameter 'key' uses "Max number", 'value' is [2,2,3,4,3]

            #"Max number" and max from [2,2,3,4,3] is 4

            #output is "Max number" 4

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row


class ReturnMax(MRJob):

    """ Mapps all numers under the same key """
    def mapper (self,key,line):


    	for num in csv_readline(line):
            yield "Max number", int(num)

    """ Receives a key and a grouped collection, from which it determines the max value """
    def reducer(self, key, value):

	       yield key, max(value)


if __name__ == '__main__':
    ReturnMax.run()
