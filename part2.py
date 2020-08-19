from mrjob.job import MRJob
import csv
import statistics #built in python library, to use mean() method


""" This program will take a CSV data file and determines the mean of all numbers in the file """


            #All values are mapped under key "Mean value"
            #"Mean value" 2
            #"Mean value" 2
            # . . . etc.
            #grouper groups "Mean value", [2,2,3,3,4]
            #same as in part1, only 1 key value pair is passed in,
            #Parameter 'key' uses "Mean value"
            #Parameter value uses [2,2,3,3,4]
            #statistics.mean[2,2,3,3,4] outputs mean value 2.8



def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row


class ReturnMean(MRJob):

    """ Mapps all numers under the same key """
    def mapper (self,key,line):
    	for num in csv_readline(line):
            yield "Mean value", int(num)

    """ Receives a key and a grouped collection, from which it determines the mean value """
    def reducer(self, key, value):
	    yield key, statistics.mean(value)


if __name__ == '__main__':
    ReturnMean.run()
