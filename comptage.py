import string
from mrjob.job import MRJob

class ComptageDesMots(MRJob):

    def mapper(self, _, ligne):
        ligneNettoyee = ligne.translate(str.maketrans('', '', string.punctuation))
        listeDeMots=ligneNettoyee.split()
        for mot in listeDeMots:
            yield mot, 1
    
    def reducer(self, key, values):
        yield key, sum(values)
