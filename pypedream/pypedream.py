class Command(object):
    def __init__(self, commandline):
        if isinstance(commandline, Command):
            self.commandline = commandline.commandline
        else:
            self.commandline = commandline

    def __rshift__(self, other):
        """self >> other"""
        pp = PartialPipeline(None, self)
        return pp >> other

    def __lshift__(self, other):
        """self << other"""
        pp = PartialPipeline(other, self)
        return pp

    def __rrshift__(self, other):
        """other >> self"""
        pp = PartialPipeline(other, self)
        return pp

    def __or__(self, other):
        """self | other"""
        print('in __or__', self, other)
        other = Command(other)
        pp = PartialPipeline(self, other)
        return pp

    def __add__(self, other):
        pass

    def format(self, *args, **kwargs):
        return self.commandline.format(*args, **kwargs)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.commandline)


class PartialPipeline(object):
    def __init__(self, input, command):
        self.input = input
        self.command = command
        self.output = None

    def __rshift__(self, other):
        self.output = other
        if self.input is not None:
            print('Read input "{}"'.format(self.input))
        print('Execute "{}"'.format(self.command))
        if self.output is None:
            # execute without retaining output
            print('without retaining output')
        else:
            print('Write into "{}"'.format(self.output))

    def __rrshift__(self, other):
        pass

    def __or__(self, other):
        """self | other"""
        other = Command(other)
        pp = PartialPipeline(self, other)
        return pp

    def __repr__(self):
        return '{}[{}, {}, {}]'.format(
            self.__class__.__name__,
            self.input, self.command, self.output)
