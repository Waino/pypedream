import subprocess
import sys
import threading

class ShovelOut(threading.Thread):
    def __init__(self, source, transform, sink, *args, **kwargs):
        self.source = source
        self.transform = transform
        self.sink = sink
        super().__init__(target=self.shovel)
        self.start()

    def shovel(self):
        with self.sink:
            for line in self.transform(self.source):
                self.sink.write(line)

def exe_func(lines):
    for line in lines:
        yield '_ ' + line

def do_stuff(inp, out):
    # create subprocesses first
    # whenever the pipe doesn't exist yet, use PIPE
    proc1 = subprocess.Popen(
        './myprog',
        stdin=inp,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        bufsize=-1)
    proc2 = subprocess.Popen(
        './myprog',
        stdin=proc1.stdout,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        bufsize=-1)
    proc3 = subprocess.Popen(
        './myprog',
        stdin=subprocess.PIPE,
        stdout=out,
        universal_newlines=True,
        bufsize=-1)
    # connect the gaps using a Shovel
    shovel1 = ShovelOut(proc2.stdout, exe_func, proc3.stdin)

    # Wait for the subprocesses to exit.
    proc1.wait()
    proc2.wait()
    shovel1.join()
    proc3.wait()

with open('testdata/large_file', 'r') as inp, \
     open('tmp/large_out', 'w') as out:
    do_stuff(inp, out)
