"""
pypedream - Utility library for scriptwriting
"""

import gzip
import pathlib
import shlex
import subprocess
import sys
import threading


class Unfilled(object):
    """ Unfilled endpoints during pipeline creation.
    Can't use None, because that means don't connect """
    def __repr__(self):
        return 'UNFILLED'
UNFILLED = Unfilled()


def unique(a, b, name):
    values = set((a, b))
    # ignore None and UNFILLED
    if None in values:
        values.remove(None)
    if UNFILLED in values:
        values.remove(UNFILLED)
    if len(values) > 1:
        raise Exception('Cannot give two separate values for "{}" '
            '({} and {})'.format(name, a, b))
    if len(values) == 0:
        if a is None or b is None:
            # prefer None (filled throwaway endpoint)
            return None
        return UNFILLED
    return values.pop()


class PypeComponent():
    def __init__(self,
                 input=UNFILLED,
                 commands=None,
                 output=UNFILLED,
                 stderr=sys.stderr,
                 parallel=None):
        self.input = input
        self.commands = [] if commands is None else commands
        self.output = output
        self.parallel = parallel
        self._stderr = None
        self.stderr(stderr)

        if all(x is not UNFILLED for x in (self.input, self.output)):
            # execute when both ends of pipeline are defined
            Execute(self)

    def new(self, **overrides):
        """ Creates a copy of self, with specified keyword attributes
        overridden. """
        kwargs = {
            'input': overrides.get('input', self.input),
            'commands': overrides.get('commands', self.commands),
            'output': overrides.get('output', self.output),
            'stderr': overrides.get('stderr', self._stderr),
            'parallel': overrides.get('parallel', self.parallel),
        }
        return PypeComponent(**kwargs)

    def __rshift__(self, other):
        """ self >> other. Pipes output of self into endpoint other. """
        if self.output != UNFILLED:
            raise Exception('Trying to set output twice for {}'.format(self))
        return self.new(output=other)

    def __lshift__(self, other):
        """ self << other. Pipes startpoint other into self. """
        if self.input != UNFILLED:
            raise Exception('Trying to set input twice for {}'.format(self))
        return self.new(input=other)

    def __rrshift__(self, other):
        """other >> self. Pipes startpoint other into self. """
        if self.input != UNFILLED:
            raise Exception('Trying to set input twice for {}'.format(self))
        return self.new(input=other)

    def __or__(self, other):
        """self | other. Pipes output of self into PypeComponent other. """
        if other.input != UNFILLED or self.output != UNFILLED:
            raise Exception(
                'Cannot use the pipe operator "|" to extend a PypeComponent '
                'that already has a filled endpoint. '
                'self.output: {out}, other.input: {inp}'.format(
                    out=self.output, inp=other.input))
        kwargs = {
            'input': self.input,
            'commands': self.commands + other.commands,
            'output': other.output,
            'stderr': unique(self._stderr, other._stderr, 'stderr'),
            'parallel': unique(self.parallel, other.parallel, 'parallel'),
        }
        return PypeComponent(**kwargs)

    def __and__(self, other):
        """self & other. Causes pypeline to be run in parallel. """
        if self.parallel is not None:
            raise Exception('Trying to set parallel twice for {}'.format(self))
        return self | other

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.commands)

    def stderr(self, stderr):
        """ Set standard error """
        self._stderr = stderr
        return self


class Command(PypeComponent):
    """ An external executable PypeComponent """
    def __init__(self, command):
        super().__init__(commands=[command])

    def __add__(self, other):
        """ Add command line arguments """
        if len(self.commands) != 1:
            raise Exception('The + operator must be used directly '
                'on individual Commands')
        command = self.commands[0] + ' ' + other
        return self.new(commands=[command])

    def format(self, *args, **kwargs):
        """ Fill in concrete arguments in a commandline
        specified as a format string. """
        if len(self.commands) != 1:
            raise Exception('The format method must be used directly '
                'on individual Commands')
        command = self.commands[0].format(*args, **kwargs)
        return self.new(commands=[command])


class Function(PypeComponent):
    """ A native Python PypeComponent """
    def __init__(self, func):
        super().__init__(commands=[func])


class ParallelPseudoCommand(PypeComponent):
    """ Causes pypeline to be run in parallel. """
    def __init__(self, context_manager):
        super().__init__(parallel=context_manager)


class Execute():
    def __init__(self, pype):
        self.pype = pype

        self.input = self._normalize_endpoint(pype.input, 'r')
        # does output need separate handling?
        # FIXME: append for debug
        self.output = self._normalize_endpoint(pype.output, 'a')
        self.err = self._normalize_endpoint(pype._stderr, 'a')
        # python commands need to be grouped
        self.grouped = list(self._group_commands(pype.commands))

        self.execute()
        if pype.parallel is None:
            print('waiting directly')
            self.wait()
        else:
            print('letting context manager do the waiting')
            pype.parallel.add_pipeline(self)

    def execute(self):
        links = [self.input]
        self.processes = []
        # create subprocesses first
        for i, (group, native) in enumerate(self.grouped):
            if not native:
                proc_input = links[-1]
                if proc_input is UNFILLED:
                    proc_input = subprocess.PIPE
                proc_output = self.output \
                    if i == len(self.grouped) - 1 else subprocess.PIPE
                proc_stderr = self.err
                proc = self._popen(proc_input, group, proc_output, proc_stderr)
                if proc_input == subprocess.PIPE:
                    # overwrite the UNFILLED with the pipe
                    links[-1] = proc.stdin
                links.append(proc.stdout)
                self.processes.append(proc)
            else:
                links.append(UNFILLED)
                self.processes.append(UNFILLED)
        if links[-1] == UNFILLED:
            links[-1] = self.output
        # connect the gaps using PythonPipelineThread
        for i, (group, native) in enumerate(self.grouped):
            if native:
                proc_input = links[i]
                proc_output = links[i + 1]
                print('PPT with {} -> {} -> {}'.format(proc_input, group, proc_output))
                proc = PythonPipelineThread(proc_input, group, proc_output)
                self.processes[i] = proc
            # else pass

    def _popen(self, proc_input, commandline, proc_output, proc_stderr):
        """ Use popen to create a subprocess """
        print('Popen with {} -> {} -> {}'.format(proc_input, commandline, proc_output))
        commandline = shlex.split(commandline)
        proc = subprocess.Popen(
            commandline,
            stdin=proc_input,
            stdout=proc_output,
            stderr=proc_stderr,
            universal_newlines=True,
            bufsize=-1)
        return proc 

    def _normalize_endpoint(self, endpoint, mode):
        ## handle various endpoints
        # turn strings into pathlib.Path
        if isinstance(endpoint, str):
            endpoint = pathlib.Path(endpoint)
        if isinstance(endpoint, pathlib.Path):
            # transparently decompress
            # TODO: other compression formats
            if str(endpoint).endswith('.gz'):
                endpoint = gzip.open(endpoint, mode)
            else:
                endpoint = endpoint.open(mode)
        # file handles: nothing needed
        # callables, iterables: nothing needed?
        return endpoint

    def _close_endpoint(self, endpoint):
        if endpoint in (sys.stdin, sys.stdout):
            # don't close standard streams
            return
        try:
            endpoint.close()
        except AttributeError:
            pass

    def _group_commands(self, commands):
        current = []
        for command in commands:
            if callable(command):
                # i.e. originally Function
                current.append(command)
            else:
                # i.e. originally Command
                if len(current) > 0:
                    yield current, True
                current = []
                yield command, False
        if len(current) > 0:
            yield current, True

    def wait(self):
        # Wait for the subprocesses to exit
        for proc in self.processes:
            print('waiting for {}'.format(proc))
            proc.wait()
        # Close endpoints if needed
        self._close_endpoint(self.input)
        self._close_endpoint(self.output)


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
        for _ in self.apply_transform():
            # consume stream
            pass

    def shovel_in(self):
        for line in self.apply_transform():
            self.sink.write(line)

    def shovel_out(self):
        stream = self.apply_transform(self.source)
        if stream is None:
            return
        for _ in stream:
            # consume stream
            pass

    def shovel_through(self):
        for line in self.apply_transform(self.source):
            self.sink.write(line)

    def wait(self):
        self.join()


class Parallel(object):
    def __init__(self):
        self.pipelines = []

    def add_pipeline(self, pipe):
        self.pipelines.append(pipe)
        return pipe

    def __enter__(self):
        return ParallelPseudoCommand(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('Parallel has:', self.pipelines)
        if exc_type is not None:
            # don't run if execption was raised
            return
        for pipe in self.pipelines:
            pipe.wait()


def run(pype_component):
    """ An alternate way to run a (sequence of) Command(s)
    without piping input or output. """
    input = None if pype_component.input == UNFILLED else pype_component.input
    output = None if pype_component.output == UNFILLED else pype_component.output
    ppipe = pype_component.new(input=input, output=output)
    return ppipe
