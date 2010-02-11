package Cache::Memcached::AnyEvent::Protocol::Text;
use strict;
use base 'Cache::Memcached::AnyEvent::Protocol';

{
    my $generator = sub {
        my $cmd = shift;
        return sub {
            my ($self, $guard, $memcached, $key, $value, $initial, $cb) = @_;
            my $fq_key = $memcached->prepare_key( $key );
            my $handle = $memcached->get_handle_for( $fq_key );
        
            $value ||= 1;
            my @command = ($cmd => $fq_key => $value);
            my $noreply = 0; # XXX - FIXME
            if ($noreply) {
                push @command, "noreply";
            }
            $handle->push_write(join(' ', @command) . "\r\n");

            if (! $noreply) {
                $handle->push_read(regex => qr{\r\n}, sub {
                    undef $guard;
                    my $data = $_[1];
                    $data =~ /^(NOT_FOUND|\w+)\r\n/;
                    $cb->($1 eq 'NOT_FOUND' ? undef : $1) if $cb;
                });
            }
        }
    };

    *decr = $generator->("decr");
    *incr = $generator->("incr");
}

sub delete {
    my ($self, $guard, $memcached, $key, $noreply, $cb) = @_;

    my $fq_key = $memcached->prepare_key( $key );
    my $handle = $memcached->get_handle_for( $key );

    my @command = (delete => $key);
    $noreply = 0; # XXX - FIXME
    if ($noreply) {
        push @command, "noreply";
    }
    $handle->push_write(join(' ', @command) . "\r\n");
    if (! $noreply) {
        $handle->push_read(regex => qr{\r\n}, sub {
            undef $guard;
            my $data = $_[1];
            my $success = $data =~ /^DELETED\r\n/;
            $cb->($success) if $cb;
        });
    }
}

sub get {
    my ($self, $guard, $memcached, $key, $cb) = @_;

    my $fq_key = $memcached->prepare_key( $key );
    my $handle = $memcached->get_handle_for( $fq_key );

    $handle->push_write( "get $fq_key\r\n" );
    $handle->push_read( line => sub {
        my ($handle, $line) = @_;
        if ($line =~ /^VALUE (\S+) (\S+) (\S+)(?: (\S+))?/)  {
            my ($rkey, $rflags, $rsize, $rcas) = ($1, $2, $3, $4);
            $handle->push_read(chunk => $rsize, sub {
                my ($key, $data) = $memcached->decode_key_value($rkey, $rflags, $_[1]);
                $handle->push_read(regex => qr{END\r\n}, cb => sub {
                    $cb->( $data );
                    undef $guard;
                } );
            });
        } else {
            confess("Unexpected line $line");
        }
    } );
}

sub get_multi {
    my ($self, $guard, $memcached, $keys, $cb) = @_;
    my $cv = AE::cv {
        undef $guard;
        $cb->( $_[0]->recv );
    };

    if (scalar @$keys == 0) {
        $cv->send({}, "no keys speficied");
        return;
    }

    my $count = $memcached->{_active_server_count};
    my %keysinserver;
    foreach my $key (@$keys) {
        my $fq_key = $memcached->prepare_key( $key );
        my $handle = $memcached->get_handle_for( $fq_key );
        my $list = $keysinserver{ $handle };
        if (! $list) {
            $keysinserver{ $handle } = $list = [ $handle, $fq_key ];
        }
        push @$list, $fq_key;
    }
   
    my %rv;
    $cv->begin( sub { $_[0]->send(\%rv) } );
    foreach my $data (values %keysinserver) {
        my ($handle, @keylist) = @$data;
        $handle->push_write( "get @keylist\r\n" );
        my $code; $code = sub {
            my ($handle, $line) = @_;
            if ($line =~ /^END(?:\r\n)?$/) {
                undef $code;
                $cv->end;
            } elsif ($line =~ /^VALUE (\S+) (\S+) (\S+)(?: (\S+))?/)  {
                my ($rkey, $rflags, $rsize, $rcas) = ($1, $2, $3, $4);
                $handle->push_read(chunk => $rsize, sub {
                    my ($key, $data) = $memcached->decode_key_value($rkey, $rflags, $_[1]);
                    $rv{ $key } = $data; # XXX whatabout CAS?
                    $handle->push_read(regex => qr{\r\n}, cb => sub { "noop" });
                    $handle->push_read(line => $code);
                } );
            } else {
                confess("Unexpected line $line");
            }
        };
        $cv->begin;
        $handle->push_read(line => $code);
    }
    $cv->end;
}

{
    my $generator = sub {
        my $cmd = shift;
        sub {
            my ($self, $guard, $memcached, $key, $value, $exptime, $noreply, $cb) = @_;
            my $fq_key = $memcached->prepare_key( $key );
            my $handle = $memcached->get_handle_for( $fq_key );

            my ($write_data, $write_len, $flags, $expires) =
                $memcached->prepare_value( $cmd, $value, $exptime );
            $handle->push_write("$cmd $fq_key $flags $expires $write_len\r\n$write_data\r\n");
            if (! $noreply) {
                $handle->push_read(regex => qr{^STORED\r\n}, sub {
                    undef $guard;
                    $cb->(1) if $cb;
                });
            }
        };
    };

    *add = $generator->("add");
    *replace = $generator->("replace");
    *set = $generator->("set");
    *append = $generator->("append");
    *prepend = $generator->("prepend");
}

sub stats {
    my ($self, $guard, $memcached, $name, $cb) = @_;

    my $cv = AE::cv {
        undef $guard;
        $cb->( $_[0]->recv );
    };

    my %rv;
    $cv->begin(sub { $_[0]->send(\%rv) });
    foreach my $server (@{ $memcached->{_active_servers} }) {
        my $handle = $memcached->get_handle( $server );

        $handle->push_write( $name ? "stats $name\r\n" : "stats\r\n" );
        my $code; $code = sub {
            my ($handle, $line) = @_;
            if ($line eq 'END') {
                $cv->end;
            } elsif ( $line =~ /^STAT (\S+) (\S+)$/) {
                $rv{ $server }->{ $1 } = $2;
                $handle->push_read( line => $code );
            }
        };
        $cv->begin;
        $handle->push_read( line => $code );
    }
    $cv->end;
}

sub version {
    my ($self, $guard, $memcached, $cb) = @_;

    # don't store guard, as we're issuing a new guarded command
    $memcached->stats( "", sub {
        my $rv = shift;
        my %version = map {
            ($_ => $rv->{$_}->{version})
        } keys %$rv;

        $cb->(\%version);
    } );
}

1;

__END__

=head1 NAME

Cache::Memcached::AnyEvent::Protocol::Text - Implements Memcached Text Protocol

=cut