import pypedream as pyd
import pathlib

# In one style, executables are declared in the beginning
myprog = pyd.Command('./myprog')

file_r = pathlib.Path('data') / 'input'
file_w = pathlib.Path('data') / 'output'

# To execute a command without retaining output,
# the user must explicitly pipe to None
print('1')
myprog >> None

print('2')
file_r >> myprog >> None
print('3')
myprog << file_r >> None

print('4')
file_r >> myprog >> file_w
print('5')
myprog << file_r >> file_w

print('6')
myprog | myprog >> None
print('7')
file_r >> myprog | myprog >> file_w

