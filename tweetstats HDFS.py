from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class tweetstats(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    SORT_VALUES=True
    def mapper(self, _, line):
        from datetime import datetime
        from collections import defaultdict
        try:
            date=line.split(',')[2]
            timestmp=datetime.strptime(date, '%m-%d-%Y %H:%M:%S')
            dct=defaultdict(int)
            dct[timestmp.hour]=1
            yield str(timestmp.year), dct
        except:
            pass
        
    def combiner(self, key, dcts):
        from collections import defaultdict
        newdct=defaultdict(list)
        for i in range(24):
            newdct[str(i)]=[]
        for dct in dcts:
            for k,v in dct.items():
                newdct[k].append(v)
                   
        yield key, newdct
        
    def reducer(self,key,values):
        from collections import defaultdict
        from datetime import datetime
        if key=='2009':
            ndays=ndays=(datetime.strptime('31.12.2009', '%d.%m.%Y')-datetime.strptime('4.5.2009', '%d.%m.%Y')).days
        elif key=='2019':
            ndays=(datetime.strptime('23.11.2019', '%d.%m.%Y')-datetime.strptime('1.1.2019', '%d.%m.%Y')).days
        else:
            ndays=(datetime.strptime(str(int(key)+1), '%Y')-datetime.strptime(key, '%Y')).days
            #in order to get the exact number of days, leap and non leap year
        totalTweet=0
        data=list(values)[0]
        for hour,v in data.items():
            totalTweet+=sum(v)
        finalStr=key
        for hour, v in data.items():
            finalStr+=' {:2.0f}'.format(sum(v)/totalTweet*100)
        finalStr+=' {:.1f}'.format(totalTweet/ndays)
        yield None, finalStr
        
if __name__=='__main__':
    tweetstats.run()
        
