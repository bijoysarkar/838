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
        for worker in self.workers:
            print "Cleaning local dirs"
            try:
                call(('ssh ubuntu@'+worker+' . /home/ubuntu/run.sh -q ; rm -r $SPARK_LOCAL_DIRS/*').split())
            except Exception as e:
                print e
            print "Dropping cache"
            try:
                call(('ssh ubuntu@'+worker+' sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"').split())
            except Exception as e:
                print e


    def spark_submit(self, mode):
        self.cleanup()
        call(('spark-submit q2_cache_evaluation.py '+mode).split())

if __name__ == "__main__":
    driver = Driver()
    driver.spark_submit(sys.argv[1])
