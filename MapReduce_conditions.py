#!/usr/bin/env python
# coding: utf-8

# In[1]:


from mr.job import MRJob
from mrjob.step import MRSTep


# In[1]:


class ConditionBreakdown (MRJob):
    def steps (self):
        return [
            
            MRStep (mapper=self.mapper_get_condition,
                   reducer=self.reducer_count_condition)
        ]
    def mapper_get_condition(self, _, line):
        (patient, encounter, code, description) = line.split('\t')
        yield description, 1
        
    def reducer_count_condition(self, key, values):
        yield key, sum(values)
if __name__=='__main__':
    ConditionBreakdown.run()


# In[ ]:




