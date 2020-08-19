from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

"""takes as input a CSV file containing comma separated words
and outputs for each word the lines that the word appears in."""

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row


class ReturnAnimalLines(MRJob):

    """ Steps will be used for further reducing and to sort the keys and their values alphabetically """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(reducer = self.reducer_2)
        ]


    """ Mapps all the comma separated values as keys, and the lines in which they appear as values """
    def mapper_1 (self,key,line):

        for item in csv_readline(line):
            yield item, line

    """ Receives each animal name as a unique key, meanwhile grouped values are all the lines
        in which the animal has appeared in. """
    def reducer_1(self, key, values):
        final_values = list(values) #values - collection is converted into a list, in order to sort them alphabetically
        final_values.sort()
        yield None, (key, final_values) #keys and values are now in a nested collection


    def reducer_2 (self,key,final_values):
        sorted_values = list(final_values) #converting keys and their respective values into a valid list
        sorted_values.sort() #sorting keys and their respective values alphabetically

        for key_value_pair in sorted_values: #yielding from the nested list the key and its collection of lines where the key word appears
            yield key_value_pair[0], key_value_pair[1]



if __name__ == '__main__':
    ReturnAnimalLines.run()
