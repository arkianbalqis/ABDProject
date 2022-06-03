#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mr.job import MRJob
from mrjob.step import MRSTep


# In[ ]:


class PatientDeathBreakdown (MRJob):
    def steps (self):
        return [
            
            MRStep (mapper=self.mapper_get_condition,
                   reducer=self.reducer_count_condition)
        ]
    def mapper_get_deathdate(self, _, line):
        (id, birthdate, deathdate, ssn, drivers, passport, gender, address, 
         city, state, county, zip, healthcare_expenses, healthcare_coverage) = line.split('\t')
        yield deathdate, 1
        
    def reducer_count_deathdate(self, key, values):
        yield key, sum(values)
if __name__=='__main__':
    PatientDeathBreakdown.run()

