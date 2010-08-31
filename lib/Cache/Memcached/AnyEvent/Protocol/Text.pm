package Cache::Memcached::AnyEvent::Protocol::Text;
use strict;
use base 'Cache::Memcached::AnyEvent::Protocol';

{
    my $generator = sub {
        my $cmd = shift;
        return sub {
            my ($self, $guard, $memcached, $key, $value, $initial, $cb) = @_;
            my $fq_key = $memcached->_prepare_key( $key );
            my $handle = $memcached->_get_handle_for( $fq_key );
        
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

    my $fq_key = $memcached->_prepare_key( $key );
    my $handle = $memcached->_get_handle_for( $fq_key );

    my @command = (delete => $fq_key);
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

    my $fq_key = $memcached->_prepare_key( $key );
    my $handle = $memcached->_get_handle_for( $fq_key );

    $handle->push_write( "get $fq_key\r\n" );
    $handle->push_read( line => sub {
        my ($handle, $line) = @_;
        if ($line =~ /^VALUE (\S+) (\S+) (\S+)(?: (\S+))?/)  {
            my ($rkey, $rflags, $rsize, $rcas) = ($1, $2, $3, $4);
            $handle->push_read(chunk => $rsize, sub {
                my $value = $_[1];
                $memcached->_decode_key_value(\$rkey, \$rflags, \$value);
                $handle->push_read(regex => qr{END\r\n}, cb => sub {
                    $cb->( $value );
                    undef $guard;
                } );
            });
        } elsif ($line =~ /^END$/) {
            $cb->( undef );
            undef $guard;
        } else {
            Carp::confess("Unexpected line $line");
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

    my %keysinserver;
    foreach my $key (@$keys) {
        my $fq_key = $memcached->_prepare_key( $key );
        my $handle = $memcached->_get_handle_for( $fq_key );
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
                    my $value = $_[1];
                    $memcached->_decode_key_value(\$rkey, \$rflags, \$value);
                    $rv{ $rkey } = $value; # XXX whatabout CAS?
                    $handle->push_read(regex => qr{\r\n}, cb => sub { "noop" });
                    $handle->push_read(line => $code);
                } );
            } else {
                Carp::confess("Unexpected line $line");
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
            my ($self, $guard, $memcached, $key, $value, $expires, $noreply, $cb) = @_;
            my $fq_key = $memcached->_prepare_key( $key );
            my $handle = $memcached->_get_handle_for( $fq_key );
            my ($len, $flags);

            $memcached->_prepare_value( $cmd, \$value, \$len, \$expires, \$flags );
            $handle->push_write("$cmd $fq_key $flags $expires $len\r\n$value\r\n");
            if (! $noreply) {
                $handle->push_read(regex => qr{^(NOT_)?STORED\r\n}, sub {
                    undef $guard;
                    $cb->($1 ? 0 : 1) if $cb;
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

sub flush_all {
    my ($self, $guard, $memcached, $delay, $noreply, $cb) = @_;

    my $cv = AE::cv {
        undef $guard;
        $cb->( $_[0]->recv ) if $cb;
    };

    $delay ||= 0;
    my @command = ('flush_all');
    push @command, $delay if ($delay);
    push @command, 'noreply' if ($noreply);
    my $command = join(' ', @command) . "\r\n";

    $cv->begin(sub { $_[0]->send(1) });
    foreach my $server (@{ $memcached->{_active_servers} }) {
        my $handle = $memcached->get_handle( $server );
        $handle->push_write( $command );
        if (! $noreply) {
            $cv->begin;
            $handle->push_read(regex => qr{^OK\r\n}, sub {
                                   $cv->end;
                               });
        }
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