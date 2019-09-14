import pypedream as pyd
import pathlib

# In one style, executables are declared in the beginning
myprog = pyd.Command('./myprog')

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

