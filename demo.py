import pypedream as pyd
import pathlib
import sys

# In one style, executables are declared in the beginning
myprog = pyd.Command('./myprog')
noinprog = pyd.Command('./noinprog')
argsprog = pyd.Command('./noinprog --a {a} --b {b}')
stderrprog = pyd.Command('./stderrprog')

file_r = pathlib.Path('testdata') / 'small_file'  #'large_file'
file_w = pathlib.Path('tmp') / 'output'
file_stderr = pathlib.Path('tmp') / 'stderr'

# To execute a command without binding stdin and stdout
# the user must explicitly pipe to None
print('1')
None >> noinprog >> None
# alternatively, use pyd.run
print('2')
pyd.run(noinprog)

# two alternate ways to specify input file
print('3')
file_r >> myprog >> None
print('4')
myprog << file_r >> None

# both ends defined
print('5')
file_r >> myprog >> file_w
print('6')
myprog << file_r >> file_w

# piping through multiple executables
print('7')
None >> noinprog | myprog >> None
print('8')
pyd.run(noinprog | myprog)
print('9')
file_r >> myprog | myprog >> file_w
print('10')
file_r >> myprog | pyd.Command('tac') | pyd.Command('rev') >> file_w

# creating a pipeline, applying it several times
print('11')
my_pipeline = myprog | myprog
file_r >> my_pipeline >> None
'testdata/a' >> my_pipeline >> file_w

# stdin, stdout and stderr of main script
print('12 disabled')
#sys.stdin >> myprog >> sys.stdout

# arguments
print('13')
'testdata/a' >> pyd.Command('./myprog --oneliner') >> 'tmp/b'
print('14')
None >> noinprog + '--add' >> None
print('15')
file_r >> myprog + '--1st' | myprog + '--2nd' >> file_w
print('16')
None >> argsprog.format(a='A', b='B') >> None

# mixing in pure python elements
print('17')
def exe_func(lines):
    for line in lines:
        yield '_ {}'.format(line)
def consumer_func(lines):
    print('first result line', next(lines))
range(10) >> pyd.Function(exe_func) >> consumer_func

# parallel execution
with pyd.Parallel() as para:
    print('18')
    None >> noinprog & para >> None
    None >> noinprog & para >> None

print('19')
# uncaught
None >> stderrprog >> None
# redirected
None >> stderrprog.stderr(file_stderr) >> None
