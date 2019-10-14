import pathlib
import shlex
import subprocess
import sys
import threading

class Unfilled(object):
    def __repr__(self):
        return 'UNFILLED'
UNFILLED = Unfilled()


class Command(object):
    def __init__(self, commandline, stderr=sys.stderr):
        self.commandline = commandline
        self.stderr(stderr)

    def new(self, **overrides):
        commandline = overrides.get('commandline', self.commandline)
        stderr = overrides.get('stderr', self._stderr)
        return self.__class__(commandline, stderr)

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
        return self.new(commandline=commandline)

    def format(self, *args, **kwargs):
        return self.new(commandline=self.commandline.format(*args, **kwargs))

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
            if not isinstance(other, Command):
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
            if str(endpoint).endswith('.gz'):
                endpoint = gzip.open(endpoint, mode)
            else:
                endpoint = endpoint.open(mode)
        # file handles: nothing needed
        # callables, iterables: nothing needed?
        # FIXME: also close the handles
        return endpoint

    def _group_commands(self, commands):
        current = []
        for command in commands:
            if callable(command.commandline):
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
        output = self._normalize_endpoint(self.output, 'a')
        # python commands need to be grouped
        grouped = list(self._group_commands(self.commands))
        links = [input]
        processes = []
        # create subprocesses first
        for i, (group, native) in enumerate(grouped):
            if not native:
                proc_input = links[-1]
                if proc_input is UNFILLED:
                    proc_input = subprocess.PIPE
                proc_output = output if i == len(grouped) - 1 else subprocess.PIPE
                print('Popen with {} -> {} -> {}'.format(proc_input, group.commandline, proc_output))
                commandline = shlex.split(group.commandline)
                proc = subprocess.Popen(
                    commandline,
                    stdin=proc_input,
                    stdout=proc_output,
                    universal_newlines=True,
                    bufsize=-1)
                if proc_input == subprocess.PIPE:
                    # overwrite the UNFILLED with the pipe
                    links[-1] = proc.stdin
                links.append(proc.stdout)
                processes.append(proc)
            else:
                links.append(UNFILLED)
                processes.append(UNFILLED)
        if links[-1] == UNFILLED:
            links[-1] = output
        # connect the gaps using PythonPipelineThread
        for i, (group, native) in enumerate(grouped):
            if native:
                proc_input = links[i]
                proc_output = links[i + 1]
                print('PPT with {} -> {} -> {}'.format(proc_input, group, proc_output))
                proc = PythonPipelineThread(proc_input, group, proc_output)
                processes[i] = proc
            # else pass
        # Wait for the subprocesses to exit
        for proc in processes:
            print('waiting for {}'.format(proc))
            proc.wait()


class PythonPipelineThread(threading.Thread):
    """ Executes a part of a pipeline
    written directly in the python script """
    def __init__(self, source, transforms, sink, *args, **kwargs):
        self.source = source
        self.transforms = transforms
        self.sink = sink
        if callable(self.sink):
            # callable sinks work better as part of transform
            self.transforms.append(self.sink)
            self.sink = None
        if all(x is None for x in (self.source, self.sink)):
            thread_target = self.no_pipes
        elif self.source is None:
            thread_target = self.shovel_in
        elif self.sink is None:
            thread_target = self.shovel_out
        else:
            thread_target = self.shovel_through
        super().__init__(target=thread_target)
        self.start()

    def apply_transform(self, stream=None):
        for transform in self.transforms:
            if stream is None:
                stream = transform()
            else:
                stream = transform(stream)
        return stream

    def no_pipes(self):
        for line in self.apply_transform():
            pass

    def shovel_in(self):
        for line in self.apply_transform():
            self.sink.write(line)

    def shovel_out(self):
        stream = self.apply_transform(self.source)
        if stream is None:
            return
        for line in stream:
            # consume stream
            pass

    def shovel_through(self):
        for line in self.apply_transform(self.source):
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
