from mrjob.job import MRJob
import csv


""" Takes as input a CSV file containing comma separated urls and finds all paths of length two in the
corresponding URL links. That is, it finds the triples of URLs (u, v, w) such that there is a link
from u to v and a link from v to w."""

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row


class ReturnPaths(MRJob):

    """ Takes the line without a key, combines the URLs into a tuple - held as values """
    def mapper (self,key,line):
        list = csv_readline(line)
        yield None, (list[0], list[1])

    """ Receives an empty key, and a value consisting of a collection, filled with tuples, containing two URLs """
    def reducer(self, key, values):
        temp = [] #temporary list used, to store a paths contained in values collection.

        for path in values:
            temp.append(path)
            for i in temp: #if temporarily stored path's end matches next path's beginning and vice versa, then yield a 3 URL path
                if path[1] == i[0]:
                    yield "paths found", ",".join([path[0],path[1],i[1]]) #join value path's beginning, end, temp path's end.
                elif path[0] == i[1]:
                    yield "paths found",",".join([i[0], path[0], path[1]]) #join temp path's beginning, value path's beginning, value path's end.



if __name__ == '__main__':
    ReturnPaths.run()
