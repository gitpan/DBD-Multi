use inc::Module::Install;

name            ('DBD-Multi');
abstract        ('Failover and Load Balancing of DBI Handles');
author          ('Dan Wright <dwright@cpan.org>');
version_from    ('lib/DBD/Multi.pm');
license         ('perl');
requires        ('perl' => 5.005);
requires        ('Class::Accessor::Fast' => 0.19 );
build_requires  ('Test::Pod::Coverage'   => 1.04 );
build_requires  ('Test::Pod'             => 1.14 );
build_requires  ('Pod::Simple');  # This is a pre-req for Test::Pod, but isn't listed.
build_requires  ('Test::More');

&WriteAll;
