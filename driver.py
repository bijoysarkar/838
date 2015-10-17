import sys
from stats import GetStatistics
from subprocess import call

class Driver:

    def __init__(self):
        self.workers = self.get_workers()
        self.get_statistics = GetStatistics()

    def get_workers(self):
        with open('/home/ubuntu/instances', 'r') as f:
            workers = f.readlines()
        return workers

    def cleanup(self):
        # Cleanup at each worker node
        for worker in self.workers:
            # Cleaning SPARK_LOCAL_DIRS
            print "Cleaning spark local dirs"
            try:
                call(('ssh ubuntu@'+worker+' . /home/ubuntu/run.sh -q ; rm -r $SPARK_LOCAL_DIRS/*').split())
            except Exception as e:
                print e
            # Dropping cache
            print "Dropping cache"
            try:
                call(('ssh ubuntu@'+worker+' sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"').split())
            except Exception as e:
                print e


    def spark_submit(self):
        self.cleanup()
        call(('spark-submit q2_cache_eval.py').split())

if __name__ == "__main__":
    driver = Driver()
    driver.spark_submit()
