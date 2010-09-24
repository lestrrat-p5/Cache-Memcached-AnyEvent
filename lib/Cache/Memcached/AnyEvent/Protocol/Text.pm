package Cache::Memcached::AnyEvent::Protocol::Text;
use strict;
use base 'Cache::Memcached::AnyEvent::Protocol';

sub _NOOP() {}

{
    my $generator = sub {
        my $cmd = shift;
        return sub {
            my ($self, $memcached, $key, $value, $initial, $cb) = @_;

            return sub {
                my $guard = shift;
                my $fq_key = $memcached->_prepare_key( $key );
                my $handle = $memcached->_get_handle_for( $key );
        
                $value ||= 1;
                my @command = ($cmd => $fq_key => $value);
                my $noreply = 0; # XXX - FIXME
                if ($noreply) {
                    push @command, "noreply";
                }
                $handle->push_write(join(' ', @command) . "\r\n");

                if ($noreply) {
                    undef $guard;
                } else {
                    $handle->push_read(line => sub {
                        undef $guard;
                        $_[1] =~ /^(NOT_FOUND|\w+)\r\n/;
                        $cb->($1 eq 'NOT_FOUND' ? undef : $1) if $cb;
                    });
                }
            }
        }
    };

    *decr = $generator->("decr");
    *incr = $generator->("incr");
}

sub delete {
    my ($self, $memcached, $key, $noreply, $cb) = @_;
    return sub {
        my $guard = shift;
        my $fq_key = $memcached->_prepare_key( $key );
        my $handle = $memcached->_get_handle_for( $key );

        my @command = (delete => $fq_key);
        $noreply = 0; # XXX - FIXME
        if ($noreply) {
            push @command, "noreply";
        }
        $handle->push_write(join(' ', @command) . "\r\n");
        if (! $noreply) {
            $handle->push_read(line => sub {
                undef $guard;
                my $data = $_[1];
                my $success = $data =~ /^DELETED\r\n/;
                $cb->($success) if $cb;
            });
        }
    };
}

sub get {
    my ($self, $memcached, $key, $cb) = @_;

    return sub {
        my $guard = shift;
        my $fq_key = $memcached->_prepare_key( $key );
        my $handle = $memcached->_get_handle_for( $key );

        $handle->push_write( "get $fq_key\r\n" );
        $handle->push_read( line => sub {
            my @bits = split /\s+/, $_[1];
            if ($bits[0] eq 'VALUE') {
                my ($rkey, $rflags, $rsize, $rcas) = @bits[1..4];
                $_[0]->push_read(chunk => $rsize, sub {
                    my $value = $_[1];
                    $memcached->_decode_key_value(\$rkey, \$rflags, \$value);
                    $handle->push_read(regex => qr{END\r\n}, cb => sub {
                        $cb->( $value );
                        undef $guard;
                    } );
                });
            } elsif ($bits[0] eq 'END') {
                $cb->( undef );
                undef $guard;
            } else {
                Carp::confess("Unexpected line $_[1]");
            }
        });
    };
}

sub get_multi {
    my ($self, $memcached, $keys, $cb) = @_;
    if (scalar @$keys == 0) {
        return sub {
            my $guard = shift;
            undef $guard;
            $cb->({}, "no keys speficied");
        }
    }

    return sub {
        my $guard = shift;

    my %keysinserver;
    foreach my $key (@$keys) {
        my $fq_key = $memcached->_prepare_key( $key );
        my $handle = $memcached->_get_handle_for( $key );
        my $list = $keysinserver{ $handle };
        if (! $list) {
            $keysinserver{ $handle } = $list = [ $handle ];
        }
        push @$list, $fq_key;
    }

    my %rv;
    my $cv = AE::cv {
        undef $guard;
        $cb->( \%rv );
    };

    foreach my $data (values %keysinserver) {
        my ($handle, @keylist) = @$data;
        $handle->push_write( "get @keylist\r\n" );
        my $code; $code = sub {
            my @bits = split /\s+/, $_[1];
            if ($bits[0] eq 'END') {
                undef $code;
                $cv->end
            } elsif ($bits[0] eq 'VALUE') {
                my ($rkey, $rflags, $rsize, $rcas) = @bits[1..4];
                $handle->push_read(chunk => $rsize, sub {
                    my $value = $_[1];
                    $memcached->_decode_key_value(\$rkey, \$rflags, \$value);
                    $rv{ $rkey } = $value; # XXX whatabout CAS?
                    $handle->push_read(line => \&_NOOP );
                    $handle->push_read(line => $code);
                } );
            } else {
                Carp::confess("Unexpected line $_[1]");
            }
        };
        $cv->begin;
        $handle->push_read(line => $code);
    }
    }
}

{
    my $generator = sub {
        my $cmd = shift;
        sub {
            my ($self, $memcached, $key, $value, $expires, $noreply, $cb) = @_;
            return sub {
                my $guard = shift;
                my $fq_key = $memcached->_prepare_key( $key );
                my $handle = $memcached->_get_handle_for( $key );
                my ($len, $flags);

                $memcached->_prepare_value( $cmd, \$value, \$len, \$expires, \$flags );
                $handle->push_write("$cmd $fq_key $flags $expires $len\r\n$value\r\n");
                if ($noreply) {
                    undef $guard;
                } else {
                    $handle->push_read(regex => qr{^(NOT_)?STORED\r\n}, sub {
                        undef $guard;
                        $cb->($1 ? 0 : 1) if $cb;
                    });
                }
            };
        };
    };

    *add = $generator->("add");
    *replace = $generator->("replace");
    *set = $generator->("set");
    *append = $generator->("append");
    *prepend = $generator->("prepend");
}

sub stats {
    my ($self, $memcached, $name, $cb) = @_;

    return sub {
        my $guard = shift;
        my %rv;
        my $cv = AE::cv {
            undef $guard;
            $cb->( \%rv );
        };

        foreach my $server (@{ $memcached->{_active_servers} }) {
            my $handle = $memcached->get_handle( $server );
            $handle->push_write( $name ? "stats $name\r\n" : "stats\r\n" );
            my $code; $code = sub {
                my @bits = split /\s+/, $_[1];
                if ($bits[0] eq 'END') {
                    $cv->end;
                } elsif ( $bits[0] eq 'STAT' ) {
                    $rv{ $server }->{ $bits[1] } = $bits[2];
                    $handle->push_read( line => $code );
                } else {
                    Carp::confess("Unexpected line $_[1]");
                }
            };
            $cv->begin;
            $handle->push_read( line => $code );
        }
    };
}

sub flush_all {
    my ($self, $memcached, $delay, $noreply, $cb) = @_;

    return sub {
        my $guard = shift;
        my $cv = AE::cv {
            undef $guard;
            $cb->(1) if $cb;
        };

        $delay ||= 0;
        my @command = ('flush_all');
        push @command, $delay if ($delay);
        push @command, 'noreply' if ($noreply);
        my $command = join(' ', @command) . "\r\n";

        $cv->begin;
        foreach my $server (@{ $memcached->{_active_servers} }) {
            my $handle = $memcached->get_handle( $server );
            $handle->push_write( $command );
            if (! $noreply) {
                $cv->begin;
                $handle->push_read(regex => qr{^OK\r\n}, sub { $cv->end });
            }
        }
        $cv->end;
    };
}

sub version {
    my ($self, $memcached, $cb) = @_;

    return sub {
        my $guard = shift;
        # don't store guard, as we're issuing a new guarded command
        $memcached->stats( "", sub {
            my $rv = shift;
            my %version = map {
                ($_ => $rv->{$_}->{version})
            } keys %$rv;
            $cb->(\%version);
        } );
    }
}

1;

__END__

=head1 NAME

Cache::Memcached::AnyEvent::Protocol::Text - Implements Memcached Text Protocol

=head1 SYNOPSIS

    use Cache::Memcached::AnyEvent;
    my $memd = Cache::Memcached::AnyEvent->new({
        ...
        protocol_class => 'Text', # Default so you can omit
    });

=head1 METHODS

=head2 add

=head2 append

=head2 decr

=head2 delete

=head2 flush_all

=head2 get

=head2 get_multi

=head2 incr

=head2 prepend

=head2 replace

=head2 set

=head2 stats

=head2 version

=cut