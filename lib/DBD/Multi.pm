package DBD::Multi;
# $Id: Multi.pm,v 1.12 2007/01/08 23:01:40 wright Exp $
use strict;

use base qw[DBD::File];

use vars qw[$VERSION $err $errstr $sqlstate $drh];

$VERSION   = '0.03';

$err       = 0;        # DBI::err
$errstr    = "";       # DBI::errstr
$sqlstate  = "";       # DBI::state
$drh       = undef;

sub driver {
    my($class, $attr) = @_;
    return $drh if $drh;
    DBI->setup_driver($class);
    my $self = $class->SUPER::driver({
        Name        => 'Multi',
        Version     => $VERSION,
        Err         => \$DBD::Multi::err,
        Errstr      => \$DBD::Multi::errstr,
        State       => \$DBD::Multi::sqlstate,
        Attribution => 'DBD::Multi, pair Networks Inc.',
    });
    # This doesn't work without formal registration with DBI
    # DBD::Multi::db->install_method('multi_do_all');
    return $self;
}

#######################################################################
package DBD::Multi::dr;
use strict;

$DBD::Multi::dr::imp_data_size = 0;
use DBD::File;
use base qw[DBD::File::dr];

sub DESTROY { shift->STORE(Active => 0) }

sub connect {
    my($drh, $dbname, $user, $auth, $attr) = @_;
    my $dbh = DBI::_new_dbh(
      $drh => {
               Name         => $dbname,
               USER         => $user,
               CURRENT_USER => $user,
              },
    );
    my @dsns =   $attr->{dsns} && ref($attr->{dsns}) eq 'ARRAY'
               ? @{$attr->{dsns}}
               : ();

    my $handler = DBD::Multi::Handler->new({
        dsources => [ @dsns ],
    });
    $handler->failed_max($attr->{failed_max})
      if exists $attr->{failed_max};
    $handler->failed_expire($attr->{failed_expire})
      if exists $attr->{failed_expire};

    $dbh->STORE(_handler => $handler);
    $dbh->STORE(handler => $handler); # temporary
    $drh->{_handler} = $handler;
    $dbh->STORE(Active => 1);
    return $dbh;
}

sub data_sources { shift->FETCH('_handler')->all_sources }

#######################################################################
package DBD::Multi::db;
use strict;

$DBD::Multi::db::imp_data_size = 0;
use base qw[DBD::File::db];

sub prepare {
    my ($dbh, $statement, @attribs) = @_;

    # create a 'blank' sth
    my ($outer, $sth) = DBI::_new_sth($dbh, { Statement => $statement });

    my $handler = $dbh->FETCH('_handler');
    $sth->STORE(_handler => $handler);

    my $_dbh = $handler->dbh;
    my $_sth;
    until ( $_sth ) {
        $_sth = $_dbh->prepare($statement, @attribs);
        unless ( $_sth ) {
            $handler->dbh_failed;
            $_dbh = $handler->dbh;
        }
    }

    $sth->STORE(NUM_OF_PARAMS => $_sth->FETCH('NUM_OF_PARAMS'));
    $sth->STORE(_dbh => $_dbh);
    $sth->STORE(_sth => $_sth);

    return $outer;
}

sub disconnect {
    my ($dbh) = @_;
    $dbh->STORE(Active => 0);
    $dbh->FETCH('_handler')->multi_do_all(sub {
        my ($dbh, $dsource) = @_;
        return unless $dbh;
        $dbh->disconnect unless UNIVERSAL::isa($dsource, 'DBI::db');
    }) if $dbh->FETCH('_handler');
    1;
}

sub commit {
    my ($dbh) = @_;
    if ( $dbh->FETCH('Active') ) {
        return $dbh->FETCH('_dbh')->commit if $dbh->FETCH('_dbh');
    }
    return;
}

sub rollback {
    my ($dbh) = @_;
    if ( $dbh->FETCH('Active') ) {
        return $dbh->FETCH('_dbh')->rollback if $dbh->FETCH('_dbh');
    }
    return;
}


sub STORE {
    my ($self, $attr, $val) = @_;
    $self->{$attr} = $val;
}

sub DESTROY { shift->disconnect }

#######################################################################
package DBD::Multi::st;
use strict;

$DBD::Multi::st::imp_data_size = 0;
use base qw[DBD::File::st];

use vars qw[@METHODS @FIELDS];
@METHODS = qw[
    bind_param
    bind_param_inout
    bind_param_array
    execute_array
    execute_for_fetch
    fetch
    fetchrow_arrayref
    fetchrow_array
    fetchrow_hashref
    fetchall_arrayref
    fetchall_hashref
    bind_col
    bind_columns
    dump_results
];

@FIELDS = qw[
    NUM_OF_FIELDS
    CursorName
    ParamValues
    RowsInCache
];

sub execute {
    my $sth  = shift;
    my $_sth = $sth->FETCH('_sth');
    my $params =   @_
                 ? $sth->{f_params} = [ @_ ]
                 : $sth->{f_params};

    $sth->finish if $sth->FETCH('Active');
    $sth->{Active} = 1;
    my $rc = $_sth->execute(@{$params});

    for my $field ( @FIELDS ) {
        my $value = $_sth->FETCH($field);
        $sth->STORE($field => $value)
          unless    ! defined $value
                 || defined $sth->FETCH($field);
    }

    return $rc;
}

sub FETCH {
    my ($sth, $attrib) = @_;
    $sth->{'_sth'}->FETCH($attrib) || $sth->{$attrib};
}

sub STORE {
    my ($self, $attr, $val) = @_;
    $self->{$attr} = $val;
}

sub rows { shift->FETCH('_sth')->rows }

sub finish {
    my ($sth) = @_;
    $sth->STORE(Active => 0);
    return $sth->FETCH('_sth')->finish;
}

foreach my $method ( @METHODS ) {
    no strict;
    *{$method} = sub { shift->FETCH('_sth')->$method(@_) };
}

#######################################################################
package DBD::Multi::Handler;
use strict;

use base qw[Class::Accessor::Fast];

__PACKAGE__->mk_accessors(qw[
    dsources
    nextid
    all_dsources
    current_dsource
    used
    failed
    failed_last
    failed_max
    failed_expire
]);

sub new {
    my ($class, $args) = @_;
    my $self     = $class->SUPER::new($args);
    $self->nextid(0) unless defined $self->nextid;
    $self->all_dsources({});
    $self->used({});
    $self->failed({});
    $self->failed_last({});
    $self->failed_max(3) unless defined $self->failed_max;
    $self->failed_expire(60*5) unless defined $self->failed_expire;
    $self->_configure_dsources;
    return $self;
}

sub all_sources {
    my ($self) = @_;
    return values %{$self->all_dsources};
}

sub add_to_pri {
    my ($self, $pri, $dsource) = @_;
    my $dsource_id = $self->nextid;
    my $dsources   = $self->dsources;
    my $all        = $self->all_dsources;

    $all->{$dsource_id} = $dsource;
    $dsources->{$pri}->{$dsource_id} = 1;

    $self->nextid($dsource_id + 1);
}

sub dbh {
    my $self = shift;
    my $dbh = $self->_connect_dsource; 
    return $dbh if $dbh;
    $self->dbh_failed;
    $self->dbh;
}

sub dbh_failed {
    my ($self) = @_;

    my $current_dsource = $self->current_dsource;
    $self->failed->{$current_dsource}++;
    $self->failed_last->{$current_dsource} = time;
}

sub _purge_old_failures {
    my ($self) = @_;
    my $now = time;
    my @all = keys %{$self->all_dsources};
    
    foreach my $dsource ( @all ) {
        next unless $self->failed->{$dsource};
        if ( ($now - $self->failed_last->{$dsource}) > $self->failed_expire ) {
            delete $self->failed->{$dsource};
            delete $self->failed_last->{$dsource};
        }
    }
}

sub _pick_dsource {
    my ($self) = @_;
    $self->_purge_old_failures;
    my $dsources = $self->dsources;
    my @pri      = sort { $a <=> $b } keys %{$dsources};

    foreach my $pri ( @pri ) {
        my $dsource = $self->_pick_pri_dsource($dsources->{$pri});
        if ( defined $dsource ) {
            $self->current_dsource($dsource);
            return;
        }
    }

    $self->used({});
    return $self->_pick_dsource
      if (grep {$self->failed->{$_} >= $self->failed_max} keys(%{$self->failed})) < keys(%{$self->all_dsources});
    die("All data sources failed!");
}

sub _pick_pri_dsource {
    my ($self, $dsources) = @_;
    my @dsources = sort { $a <=> $b } keys %{$dsources};
    my @used     = grep { exists $self->used->{$_} } @dsources;
    my @failed   = grep { exists($self->failed->{$_}) && $self->failed->{$_} >= $self->failed_max } @dsources;

    # We've used them all and they all failed. Escallate.
    return if @used == @dsources && @failed == @dsources;
    
    # We've used them all but some are good. Purge and reuse.
    delete @{$self->used}{@dsources} if @used == @dsources;

    foreach my $dsource ( @dsources ) {
        next if    $self->failed->{$dsource}
                && $self->failed->{$dsource} >= $self->failed_max;
        next if $self->used->{$dsource};

        $self->used->{$dsource} = 1;
        return $dsource;
    }
    return;
}

sub _configure_dsources {
    my ($self) = @_;
    my $dsources = $self->dsources;
    $self->dsources({});

    while ( my $pri = shift @{$dsources} ) {
        my $dsource = shift @{$dsources} or last;
        $self->add_to_pri($pri => $dsource);
    }
}

sub _connect_dsource {
    my ($self, $dsource) = @_;
    unless ( $dsource ) {
        $self->_pick_dsource;
        $dsource = $self->all_dsources->{$self->current_dsource};
    }

    return $dsource if UNIVERSAL::isa($dsource, 'DBI::db');

    my $dbh = DBI->connect_cached(@{$dsource});
    return $dbh;
}

sub connect_dsource {
    my ($self, $dsource) = @_;
    $self->_connect_dsource($dsource);
}

sub multi_do_all {
    my ($self, $code) = @_;

    my @all = values %{$self->all_dsources};

    foreach my $source ( @all ) {
        my $dbh = $self->connect_dsource($source);
        next unless $dbh;
        if ( $dbh->{handler} ) {
            $dbh->{handler}->multi_do_all($code, $source);
            next;
        }
        $code->($dbh);
    }
}

1;
__END__

=head1 NAME

DBD::Multi - Manage Multiple Data Sources with Failover and Load Balancing

=head1 SYNOPSIS

  use DBI;

  my $other_dbh = DBI->connect(...);

  my $dbh = DBI->connect( 'dbi:Multi:', undef, undef, {
      dsns => [ # in priority order
          10 => [ 'dbi:SQLite:read_one.db', '', '' ],
          10 => [ 'dbi:SQLite:read_two.db', '', '' ],
          20 => [ 'dbi:SQLite:master.db',   '', '' ],
          30 => $other_dbh,
      ],
      # optional
      failed_max    => 1,     # short credibility
      failed_expire => 60*60, # long memory
  });

=head1 DESCRIPTION

This software manages multiple database connections for the purposes of load
balancing and simple failover procedures. It acts as a proxy between your code
and your available databases.

Although there is some code intended for read/write operations, this should be
considered EXPIREMENTAL.  This module is primary intended for read-only
operations (where some other application is being used to handle replication).
This software does not prevent write operations from being executed.  This is
left up to the user. (One suggestion is to make sure the user your a connecting
to the db as has privileges sufficiently restricted to prevent updates).

The interface is nearly the same as other DBI drivers with one notable
exception.

=head2 Configuring DSNs

Specify an attribute to the C<connect()> constructor, C<dsns>. This is a list
of DSNs to configure. The configuration is given in pairs. First comes the
priority of the DSN. Second is the DSN.

The priorities specify which connections should be used first (lowest to
highest).  As long as the lowest priority connection is responding, the higher
priority connections will never be used.  If multiple connections have the same
priority, then one connection will be chosen randomly for each operation.  Note
that the random DB is chosen when the statement is prepared.   Therefore
executing multiple queries on the same prepared statement handle will always
run on the same connection.

The second parameter can either be a DBI object or a list of parameters to pass
to the DBI C<connect()> instructor.   If a set of parameters is given, then
DBD::Multi will be able to attempt re-connect in the event that the connection
is lost.   If a DBI object is used, the DBD::Multi will give up permanently
once that connection is lost.

=head2 Configuring Failures

By default a data source will not be tried again after it has failed
three times. After five minutes that failure status will be removed and
the data source may be tried again for future requests.

To change the maximum number of failures allowed before a data source is
deemed failed, set the C<failed_max> parameter. To change the amount of
time we remember a data source as being failed, set the C<failed_expire>
parameter in seconds.

=head1 SEE ALSO

L<DBD::Multiplex>,
L<DBI>,
L<perl>.

=head1 AUTHOR

Initially written by Casey West and Dan Wright for pair Networks, Inc.
(www.pair.com)

Maintained by Dan Wright.  <F<DWRIGHT@CPAN.ORG>>.

=cut

