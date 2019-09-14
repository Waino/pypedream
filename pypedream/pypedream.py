class Unfilled(object):
    def __repr__(self):
        return 'UNFILLED'
UNFILLED = Unfilled()


class Command(object):
    def __init__(self, commandline):
        if isinstance(commandline, Command):
            self.commandline = commandline.commandline
        else:
            self.commandline = commandline

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
        pass

    def format(self, *args, **kwargs):
        return self.commandline.format(*args, **kwargs)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.commandline)


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

    def _execute(self):
        if self.input is not None:
            print('Read input "{}"'.format(self.input))
        for command in self.commands:
            print('Execute "{}"'.format(command))
        if self.output is None:
            # execute without retaining output
            print('without retaining output')
        else:
            print('Write into "{}"'.format(self.output))


def run(partial_pipeline):
    if isinstance(partial_pipeline, Command):
        partial_pipeline = PartialPipeline(commands=[partial_pipeline])
    input = None if partial_pipeline.input == UNFILLED else partial_pipeline.input
    output = None if partial_pipeline.output == UNFILLED else partial_pipeline.output
    pp = PartialPipeline(input, partial_pipeline.commands, output)
    return pp
