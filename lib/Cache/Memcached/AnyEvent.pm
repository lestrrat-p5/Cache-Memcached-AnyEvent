package Cache::Memcached::AnyEvent;
use strict;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use Carp;
use String::CRC32;

use constant +{
    HAVE_ZLIB => eval { require Compress::Zlib; 1 },
    F_STORABLE => 1,
    F_COMPRESS => 2,
    COMPRESS_SAVINGS => 0.20,
};

our $VERSION = '0.00008';

sub new {
    my $class = shift;
    my $self  = bless {
        compess_threshold => 10_000,
        hash_cb => \&_modulo_hasher,
        protocol => undef,
        protocol_class => 'Text',
        servers => undef,
        namespace => undef,
        @_ == 1 ? %{$_[0]} : @_,
        _active_servers => [],
        _active_server_count => 0,
        _is_connected => undef,
        _is_connecting => undef,
        _queue => [],
        _server_handles => undef,
    }, $class;

    $self->{protocol} ||= $self->_build_protocol;

    return $self;
}

sub _build_protocol {
    my $self = shift;
    my $p_class = $self->{protocol_class};
    if ($p_class !~ s/^\+//) {
        $p_class = "Cache::Memcached::AnyEvent::Protocol::$p_class";
    }

    $p_class =~ s/[^\w:_]//g; # cleanse

    eval "require $p_class";
    Carp::confess $@ if $@;
    return $p_class->new(memcached => $self);
}

BEGIN {
    foreach my $attr qw(compress_threshold servers namespace) {
        eval <<EOSUB;
            sub $attr {
                my \$self = shift;
                my \$ret  = \$self->{$attr};
                if (\@_) {
                    \$self->{$attr} = shift;
                }
                return \$ret;
            }
EOSUB
        Carp::confess if $@;
    }

    foreach my $attr qw(protocol) {
        eval <<EOSUB;
            sub $attr {
                my \$self = shift;
                my \$ret  = \$self->{$attr};
                if (\@_) {
                    my \$obj = shift;
                    my \$class = ref \$obj;
                    \$self->{$attr} = \$obj;
                    \$self->{${attr}_class} = \$class;
                }
                return \$ret;
            }
EOSUB
        Carp::confess if $@;
        eval <<EOSUB;
            sub ${attr}_class {
                my \$self = shift;
                my \$ret  = \$self->{${attr}_class};
                if (\@_) {
                    \$self->{${attr}_class} = shift;
                    \$self->{$attr} = \$self->_build_${attr};
                }
                return \$ret;
            }
EOSUB
        Carp::confess if $@;
    }
}

sub connect {
    my $self = shift;

    return if $self->{_is_connecting} || $self->{_is_connected};

    $self->{_is_connecting} = {};

    $self->{_active_servers} = [];
    $self->{_active_server_count} = 0;
    my %handles;
    my $connect_cv = AE::cv {
        $self->{_server_handles} = \%handles;
        delete $self->{_is_connecting};
        $self->{_is_connected} = 1;
        $self->drain_queue;
    };

    foreach my $server ( @{ $self->{ servers } }) {
        $connect_cv->begin;

        my ($host, $port) = split( /:/, $server );
        $port ||= 11211;

print "# Connecting to $server\n";
        $self->{_is_connecting}->{$server} = tcp_connect $host, $port, sub {
            my ($fh, $host, $port) = @_;

            delete $self->{_is_connecting}->{$server}; # thanks, buddy
            if (! $fh) {
                # connect failed
                warn "failed to connect to $server";
            } else {
                my $h; $h = AnyEvent::Handle->new(
                    fh => $fh,
                    on_drain => sub {
                        my $h = shift;
                        if (defined $h->{wbuf} && $h->{wbuf} eq "") {
                            delete $h->{wbuf}; $h->{wbuf} = "";
                        }
                        if (defined $h->{rbuf} && $h->{rbuf} eq "") {
                            delete $h->{rbuf}; $h->{rbuf} = "";
                        }
                    },
                    on_eof => sub {
                        my $h = delete $handles{$server};
                        $h->destroy();
                        undef $h;
                    },
                    on_error => sub {
                        my $h = delete $handles{$server};
                        $h->destroy();
                        undef $h;
                    },
                );

                push @{$self->{_active_servers}}, $server;
                $self->{_active_server_count}++;
                $handles{ $server } = $h;
                $self->protocol->prepare_handle( $fh );
            }

            $connect_cv->end;
        };
    }
}

sub add {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $exptime, $noreply) = @args;
    $self->push_queue( 
        $self->protocol, 'add', $key, $value, $exptime, $noreply, $cb );
}

sub decr {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $initial) = @args;
    $self->push_queue( $self->protocol, 'decr', $key, $value, $initial, $cb );
}

sub delete {
    my ($self, @args) = @_;
    my $cb       = pop @args if ref $args[-1] eq 'CODE';
    my $noreply  = !defined $cb;
    $self->push_queue( $self->protocol, 'delete', @args, $noreply, $cb );
}

sub get {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    $self->push_queue( $self->protocol, 'get_multi', 'single', \@args, $cb );
}

sub get_handle { shift->{_server_handles}->{ $_[0] } }

sub get_multi {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    $self->push_queue( $self->protocol, 'get_multi', 'multi', \@args, $cb );
}

sub incr {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $initial) = @args;
    $self->push_queue( $self->protocol, 'incr', $key, $value, $initial, $cb );
}

sub replace {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $exptime, $noreply) = @args;
    $self->push_queue( $self->protocol, 'replace', $key, $value, $exptime, $noreply, $cb );
}

sub set {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $exptime, $noreply) = @args;
    $self->push_queue( $self->protocol, 'set', $key, $value, $exptime, $noreply, $cb);
}

sub append {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    $self->push_queue( $self->protocol, 'append', @args, undef, undef, $cb);
}

sub prepend {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    $self->push_queue( $self->protocol, 'prepend', @args, undef, undef, $cb);
}

sub stats {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($name) = @args;
    $self->push_queue( $self->protocol, 'stats', $name, $cb );
}

sub version {
    my ($self, $cb) = @_;
    $self->push_queue( $self->protocol, 'version', $cb);
}

sub push_queue {
    my ($self, @args) = @_;
    push @{$self->{queue}}, [ @args ];
    $self->drain_queue unless $self->{_is_draining};
}

sub drain_queue {
    my $self = shift;
    if (! $self->{_is_connected}) {
        if ($self->{_is_connecting}) {
            return;
        }
        $self->connect;
        return;
    }

    if ($self->{_is_draining}) {
        return;
    }

    if (my $next = shift @{$self->{queue}}) {
        my ($proto, $method, @args) = @$next;
        $self->{_is_draining}++;
        my $guard = AnyEvent::Util::guard {
            my $t; $t = AE::timer 0, 0, sub {
                $self->{_is_draining}--;
                undef $t;
                $self->drain_queue;
            };
        };
        $proto->$method($guard, $self, @args);
    }
}

sub disconnect {
    my $self = shift;

    my $handles = delete $self->{_server_handles};
    foreach my $handle ( values %$handles ) {
        if ($handle) {
            eval {
                $handle->stop_read;
                $handle->push_shutdown();
                $handle->destroy();
            };
        }
    }

    delete $self->{_is_connecting};
    delete $self->{_is_connected};
    delete $self->{_is_draining};
}

sub DESTROY {
    my $self = shift;
    $self->disconnect;
}
    
sub get_handle_for {
    my ($self, $key) = @_;
    my $servers   = $self->{_active_servers};
    my $i         = $self->{hash_cb}->($key, $self);
    my $handle    = $self->get_handle( $servers->[ $i ] );
    if (! $handle) {
        die "Could not find handle for $key";
    }

    return $handle;
}

sub prepare_key {
    my ($self, $key) = @_;
    if (my $ns = $self->{namespace}) {
        $key = $ns . $key;
    }
    return $key;
}

sub decode_key_value {
    my ($self, $key, $flags, $data) = @_;

    if (my $ns = $self->{namespace}) {
        $key =~ s/^$ns//;
    }

    if ($flags & F_COMPRESS() && HAVE_ZLIB()) {
        $data = Compress::Zlib::memGunzip($data);
    }
    if ($flags & F_STORABLE()) {
        $data = Storable::thaw($data);
    }
    return ($key, $data);
}

sub decode_key {
    my ($self, $key) = @_;

    if (my $ns = $self->{namespace}) {
        $key =~ s/^$ns//;
    }
    return $key;
}

sub prepare_value {
    my ($self, $cmd, $value, $exptime) = @_;

    my $flags = 0;
    if (ref $value) {
        $value = Storable::nfreeze($value);
        $flags |= F_STORABLE();
    }

    my $len = bytes::length($value);
    my $threshold = $self->compress_threshold;
    my $compressable = 
        ($cmd ne 'append' && $cmd ne 'prepend') &&
        $threshold && 
        HAVE_ZLIB() &&
        $len >= $threshold
    ;
    if ($compressable) {
        my $c_val = Compress::Zlib::memGzip($value);
        my $c_len = bytes::length($c_val);

        if ($c_len < $len * ( 1 - COMPRESS_SAVINGS() ) ) {
            $value = $c_val;
            $len = $c_len;
            $flags |= F_COMPRESS();
        }
    }
    $exptime = int($exptime || 0);

    return ($value, $len, $flags, $exptime);
}

sub _modulo_hasher {
    my ($key, $memcached) = @_;
    my $count = $memcached->{_active_server_count};
    return ((String::CRC32::crc32($key) >> 16) & 0x7fff) % $count;
}


1;

__END__

=head1 NAME

Cache::Memcached::AnyEvent - AnyEvent Compatible Memcached Client 

=head1 SYNOPSIS

    use Cache::Memcached::AnyEvent;

    my $memd = Cache::Memcached::AnyEvent->new({
        servers => [ '127.0.0.1:11211' ],
        compress_threshold => 10_000,
        namespace => 'myapp.',
    });

    $memd->get( $key, sub {
        my ($value) = @_;
        warn "got $value for $key";
    });

=head1 DESRIPTION

WARNING: ALPHA QUALITY CODE!

This module implements the memcached protocol as a AnyEvent consumer, and it implments both for text and binary protocols.

=head1 RATIONALE

There's another alternative L<AnyEvent> memcached client, L<AnyEvent::Memcached> which is perfectly fine, and I have nothing against you using that module, but I had specific itches to scratch:

=over 4

=item Prerequisites

This module, L<Cache::Memcached::AnyEvent>, requires the bare minimum prerequisites to install.

There were more than a few modules that get installed for L<AnyEvent::Memcached> (including some modules that I had to install solely for it) and I wanted to avoid it.

=item Binary Protocol

I was in the mood to implement the binary protocol. I don't believe it's a requirement to do anything, so this is purely a whim.

=item Unimplemented Methods

get_multi and the like are not implemented yet on L<AnyEvent::Memcached>.

=back

=head1 METHODS

=head2 new

=head2 add($key, $value[, $exptime, $noreply], $cb->($rc))

=head2 connect()

Explicitly connects to each server given. You DO NOT need to call this
explicitly.

=head2 decr($key, $delta[, $initial], $cb->($value))

=head2 delete($key, $cb->($rc))

=head2 disconnect()

=head2 get($key, $cb->($value))

=head2 get_handle( $host_port )

=head2 get_multi(\@keys, $cb->(\%values));

=head2 incr($key, $delta[, $initial], $cb->($value))

=head2 protocol($object)

=head2 protocol_class($class)

=head2 replace($key, $value[, $exptime, $noreply], $cb->($rc))

=head2 remove($key, $cb->($rc))

Alias to delete

=head2 servers()

=head2 set($key, $value[, $exptime, $noreply], $cb->($rc))

=head2 stats($cmd, $cb->(\%stats))

=head2 version($cb->(\%result))

=head1 TODO

=over 4

=item Binary stats is not yet implemented.

=back

=head1 AUTHOR

Daisuke Maki C<< <daisuke@endeworks.jp> >>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut