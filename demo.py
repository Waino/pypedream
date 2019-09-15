import pypedream as pyd
import pathlib
import sys

# In one style, executables are declared in the beginning
myprog = pyd.Command('./myprog')
argsprog = pyd.Command('./argsprog --a {a} --b {b}')

file_r = pathlib.Path('data') / 'input'
file_w = pathlib.Path('data') / 'output'

# To execute a command without binding stdin and stdout
# the user must explicitly pipe to None
print('1')
None >> myprog >> None
# alternatively, use pyd.run
print('2')
pyd.run(myprog)

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
None >> myprog | myprog >> None
print('8')
pyd.run(myprog | myprog)
print('9')
file_r >> myprog | myprog >> file_w
print('10')
file_r >> myprog | pyd.Command('./second') | pyd.Command('./third') >> file_w

# creating a pipeline, applying it several times
print('11')
my_pipeline = myprog | myprog
file_r >> my_pipeline >> None
None >> my_pipeline >> file_w

# stdin, stdout and stderr of main script
print('12')
sys.stdin >> myprog >> sys.stdout

# arguments
print('13')
'data/a' >> pyd.Command('./myprog --oneliner') >> 'data/b'
print('14')
None >> myprog + '--add' >> None
print('15')
file_r >> myprog + '--1st' | myprog + '--2nd' >> file_w
print('16')
None >> argsprog.format(a='A', b='B') >> None

# mixing in pure python elements
print('17')
def exe_func(lines):
    for line in lines:
        yield '_' + line
def consumer_func(lines):
    print('result', len(lines), lines[0])
range(10) >> pyd.Command(exe_func) >> consumer_func
