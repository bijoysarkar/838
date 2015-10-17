import os
import sys
import time
from subprocess import check_output

class GetStatistics:

    def __init__(self):
        self.workers = self.get_workers()

    def get_workers(self):
        with open('/home/ubuntu/instances', 'r') as f:
	    workers = f.readlines()
        return workers

    # Obtain disk data from each worker node
    def get_disc_data(self):
        vda_read_total = 0
        vda_write_total = 0
        for worker in self.workers:
    	    disc_data = check_output(("ssh ubuntu@"+worker+" cat /proc/diskstats").split())
            vda_read, vda_write = self.parse_disc_data(disc_data)
            vda_read_total = vda_read_total + vda_read
            vda_write_total = vda_write_total + vda_write
        return vda_read_total*512.0/1024/1024/1024, vda_write_total*512.0/1024/1024/1024

    def parse_disc_data(self, data):
        lines = data.strip().split('\n')
        vda_parts = lines[-2].split()
        vda1_parts = lines[-1].split()
        if vda_parts[2]=='vda' and vda1_parts[2]=='vda1':
            return (int(vda_parts[5])+int(vda1_parts[5])), (int(vda_parts[9])+int(vda1_parts[9]))
        else:
            print "Error"

    # Obtain network data from each worker node
    def get_net_data(self):
        network_read_total = 0
        network_write_total = 0
        for worker in self.workers:
    	    net_data = check_output(("ssh ubuntu@"+worker+" cat /proc/net/dev").split())
            network_read, network_write = self.parse_net_data(net_data)
            network_read_total = network_read_total + network_read
            network_write_total = network_write_total + network_write
        return network_read_total*1.0/1024/1024/1024, network_write_total*1.0/1024/1024/1024

    def parse_net_data(self, data):
        lines = data.strip().split('\n')
        if lines[2].strip().startswith('eth0'):
            parts = lines[2].split(':')[1].split()
            return int(parts[0]), int(parts[8])
        else:
            print "Error"

if __name__ == "__main__":
    get_statistics = GetStatistics()
    get_statistics.cleanup()
    print get_statistics.get_disc_data()
    print get_statistics.get_net_data()
