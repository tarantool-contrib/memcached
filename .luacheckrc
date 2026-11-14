std = 'luajit'
globals = {'box', 'tonumber64', '_TARANTOOL', 'misc'}
ignore = {'212/self', '411', '421', '431', '432'}
include_files = {'**/*.lua'}
exclude_files = {'.rocks/**/*.lua', '_build/**/*.lua', '.git/**/*.lua'}
