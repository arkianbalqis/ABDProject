#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mr.job import MRJob
from mrjob.step import MRSTep


# In[ ]:


class CareplansBreakdown (MRJob):
    def steps (self):
        return [
            
            MRStep (mapper=self.mapper_get_condition,
                   reducer=self.reducer_count_condition)
        ]
    def mapper_get_condition(self, _, line):
        (ID, patient, encounter, code, description, reasoncode, reasondescription) = line.split('\t')
        yield description, 1
        
    def reducer_count_condition(self, key, values):
        yield key, sum(values)
if __name__=='__main__':
    CareplansBreakdown.run()

