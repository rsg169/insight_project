# WORD_COUNT.PY: This script is an entry point for a job submission.

import re
import sys

from collections import Counter

from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob

class WordCountJob(CCSparkJob):
    """ Word count (frequency list) from texts in Common Crawl WET files"""

    name = "WordCount"

    # output is <word, <term_frequency, document_frequency>>
    output_schema = StructType([
        StructField("key", StringType(), True),
        StructField("val", StructType([
            StructField("tf", LongType(), True),
            StructField("df", LongType(), True)]), True)
    ])

    # simple Unicode-aware tokenization
    # (not suitable for CJK languages)
    #word_pattern = re.compile('\w+', re.UNICODE)
    word_pattern = None

    # This method compiles a regular expression based on the entered search term, of the form r'\b{TERM}\b' 
    @staticmethod
    def set_search_term(a):
        WordCountJob.word_pattern = re.compile(r'\b'+re.escape(a)+r'\b', re.ASCII | re.IGNORECASE)

    @staticmethod
    def reduce_by_key_func(a, b):
        # sum values of tuple <term_frequency, document_frequency>
        return ((a[0] + b[0]), (a[1] + b[1]))

    def process_record(self, record):
        if not self.is_wet_text_record(record):
            return
        data = record.content_stream().read().decode('utf-8')
        words = map(lambda w: w.lower(),
                    WordCountJob.word_pattern.findall(data))
        for word, count in Counter(words).items():
            yield word, (count, 1)


if __name__ == '__main__':
    WordCountJob.set_search_term(sys.argv[3])
    job = WordCountJob()
    job.run()
