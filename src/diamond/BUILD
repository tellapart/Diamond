python_library(
  name = '_diamond',
  sources = globs('*.py'),
  dependencies = [
    '3rdparty/python:configobj',
    '3rdparty/python:psutil',
    'src/diamond/handler',
    'src/collectors',
  ],
)

python_binary(
  name='diamond',
  entry_point = 'diamond.main:main',
  dependencies = [
    ':_diamond'
  ],
)
