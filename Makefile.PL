use inc::Module::Install;

name            ('DBD-Multi');
abstract        ('Failover and Load Balancing of DBI Handles');
author          ('Dan Wright <dwright@cpan.org>');
version_from    ('lib/DBD/Multi.pm');
license         ('perl');
requires        ('perl' => 5.005);
build_requires  ('Test::More');
build_requires  ('DBD::SQLite');

&WriteAll;
