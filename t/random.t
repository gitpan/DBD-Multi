# vim: ft=perl
use Test::More 'no_plan';
use strict;
$^W = 1;

# Test that two dbs with the same priority are actually randomly selected.

use DBI;
use DBD::SQLite;
use DBD::Multi;

# Set up the first DB with a value of 1
 my $dbh_1 = DBI->connect("dbi:SQLite:one.db");
 is $dbh_1->do("CREATE TABLE multi(id int)"), '0E0', 'do create successful';
 is($dbh_1->do("INSERT INTO multi VALUES(1)"), 1, 'insert via do works');

# And the second DB with the value of 2
 $dbh_1 = DBI->connect("dbi:SQLite:two.db");
 is $dbh_1->do("CREATE TABLE multi(id int)"), '0E0', 'do create successful';
 is($dbh_1->do("INSERT INTO multi VALUES(2)"), 1, 'insert via do works');


my $c = DBI->connect('DBI:Multi:', undef, undef, {
    dsns => [
        1 => ['dbi:SQLite:one.db', '',''],
        1 => ['dbi:SQLite:two.db','',''],
    ],
});

my ($one_cnt,$two_cnt) = (0,0);
for (1..100) {
    my $val = $c->selectrow_array("SELECT id FROM multi");
    $one_cnt++ if ($val == 1);
    $two_cnt++ if ($val == 2);
}

ok($one_cnt,  "first db with same priority was selected through random process ($one_cnt/100)");
ok($two_cnt, "second db wth  same priority was selected through random process ($two_cnt/100)");


unlink "$_.db" for qw[one two];
