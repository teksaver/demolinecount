from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "mots", len(line.split())
        yield "lignes", 1

    def reducer(self, key, values):
        yield key, sum(values)
