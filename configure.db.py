
conf_file = '/etc/yum.repos.d/CentOS-Base.repo'

with open(conf_file, 'r') as f:
    buf = f.readlines()

with open(conf_file, 'w') as f:
    for line in buf:
        if line == "[base]\n" or line == "[updates]\n":
            line = line + "exclude=postgresql*\n"
        f.write(line)