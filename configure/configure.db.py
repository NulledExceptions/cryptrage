import configparser


for file, sections in zip(['/etc/yum.repos.d/CentOS-Base.repo', '/etc/yum/yum-cron.conf'],
                          [['base', 'updates'], ['base']]):
    config = configparser.ConfigParser()
    config.read(file)
    for section in sections:
        config[section]['exclude'] += ' postgresql* '
    with open(file, 'w') as f:
        config.write(f)