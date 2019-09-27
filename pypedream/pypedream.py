import subprocess
import sys
import threading

class Unfilled(object):
    def __repr__(self):
        return 'UNFILLED'
UNFILLED = Unfilled()


class Command(object):
    def __init__(self, commandline, stderr=sys.stderr):
        if isinstance(commandline, Command):
            self.commandline = commandline.commandline
            stderr = commandline._stderr
        else:
            self.commandline = commandline
        self.stderr(stderr)

    def __rshift__(self, other):
        """self >> other"""
        pp = PartialPipeline(commands=[self])
        return pp >> other

    def __lshift__(self, other):
        """self << other"""
        pp = PartialPipeline(input=other, commands=[self])
        return pp

    def __rrshift__(self, other):
        """other >> self"""
        pp = PartialPipeline(input=other, commands=[self])
        return pp

    def __or__(self, other):
        """self | other"""
        pp = PartialPipeline(commands=[self])
        return pp | other

    def __add__(self, other):
        commandline = self.commandline + ' ' + other
        return Command(commandline, self._stderr)

    def format(self, *args, **kwargs):
        return Command(self.commandline.format(*args, **kwargs),
                       self._stderr)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.commandline)

    def stderr(self, stderr):
        # TODO: opening of file if needed
        self._stderr = stderr


class PartialPipeline(object):
    def __init__(self, input=UNFILLED, commands=UNFILLED, output=UNFILLED):
        self.input = input
        self.commands = [] if commands == UNFILLED else commands
        self.output = output
        if all(x is not UNFILLED for x in (self.input, self.output)):
            self._execute()

    def __rshift__(self, other):
        """self >> other"""
        assert self.output == UNFILLED
        pp = PartialPipeline(self.input, self.commands, other)
        return pp

    def __rrshift__(self, other):
        """other >> self"""
        assert self.input == UNFILLED
        pp = PartialPipeline(other, self.commands, self.output)
        return pp

    def __or__(self, other):
        """self | other"""
        if isinstance(other, PartialPipeline):
            assert self.output == UNFILLED
            assert other.input == UNFILLED
            commands = self.commands + other.commands
            pp = PartialPipeline(self.input, commands, other.output)
        else:
            other = Command(other)
            commands = self.commands + [other]
            pp = PartialPipeline(self.input, commands, self.output)
        return pp

    def __repr__(self):
        return '{}[{}, {}, {}]'.format(
            self.__class__.__name__,
            self.input, self.commands, self.output)

    def _dummy_execute(self):
        if self.input is not None:
            print('Read input "{}"'.format(self.input))
        for command in self.commands:
            print('Execute "{}"'.format(command))
        if self.output is None:
            print('without retaining output')
        else:
            print('Write into "{}"'.format(self.output))

    def _normalize_endpoint(self, endpoint, mode):
        ## handle various endpoints
        # turn strings into pathlib.Path
        if isinstance(endpoint, str):
            endpoint = pathlib.Path(endpoint)
        if isinstance(endpoint, pathlib.Path):
            # transparently decompress
            if endpoint.endswith('.gz'):
                endpoint = gzip.open(endpoint, mode)
        # file handles: nothing needed
        # callables, iterables: nothing needed?
        return endpoint

    def _group_commands(self, commands):
        current = []
        for command in commands:
            if isinstance(command.commandline, callable):
                # also unwrapping
                current.append(command.commandline)
            else:
                if len(current) > 0:
                    yield current, True
                current = []
                yield command, False
        if len(current) > 0:
            yield current, True

    def _execute(self):
        input = self._normalize_endpoint(self.input, 'r')
        # does output need separate handling?
        # FIXME: append
        output = self._normalize_endpoint(self.output, 'w+')
        # python commands need to be grouped
        grouped = list(self._group_commands(self.commands))
        links = [input]
        processes = []
		# create subprocesses first
        for i, (group, native) in enumerate(grouped):
            if not native:
                proc_input = links[-1]
                if proc_input is None:
                    proc_input = subprocess.PIPE
                proc_output = output if i == len(grouped) - 1 else subprocess.PIPE
                proc = subprocess.Popen(
                    group.commandline,
                    stdin=proc_input,
                    stdout=proc_output,
                    universal_newlines=True,
                    bufsize=-1)
                if proc_input == subprocess.PIPE:
                    # overwrite the None with the pipe
                    links[-1] = proc.stdin
                links.append(proc.stdout)
                processes.append(proc)
            else:
                links.append(None)
                processes.append(None)
        # connect the gaps using PythonPipelineThread
        for i, (group, native) in enumerate(grouped):
            if native:
                proc_input = links[i]
                proc_output = links[i + 1]
                proc = PythonPipelineThread(proc_input, group, proc_output)
                processes.append(proc)
            # else pass
        # Wait for the subprocesses to exit.
        for proc in processes:
            proc.wait()


class PythonPipelineThread(threading.Thread):
    """ Executes a part of a pipeline
    written directly in the python script """
    def __init__(self, source, transform, sink, *args, **kwargs):
        self.source = source
        self.transform = transform
        self.sink = sink
        if all(x is None for x in (self.source, self.target)):
            raise Exception('Python command cannot have both ends None')
        if self.source is None:
            thread_target = self.shovel_in
        elif self.target is None:
            thread_target = self.shovel_out
        else:
            thread_target = self.shovel_through
        super().__init__(target=thread_target)
        self.start()

    def shovel_in(self):
        with self.sink:
            for line in self.transform():
                self.sink.write(line)

    def shovel_out(self):
        for line in self.transform(self.source):
            pass

    def shovel_through(self):
        with self.sink:
            for line in self.transform(self.source):
                self.sink.write(line)

    def wait(self):
        self.join()


def run(partial_pipeline):
    if isinstance(partial_pipeline, Command):
        partial_pipeline = PartialPipeline(commands=[partial_pipeline])
    input = None if partial_pipeline.input == UNFILLED else partial_pipeline.input
    output = None if partial_pipeline.output == UNFILLED else partial_pipeline.output
    pp = PartialPipeline(input, partial_pipeline.commands, output)
    return pp
