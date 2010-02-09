package Cache::Memcached::AnyEvent;
use strict;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use Carp qw(confess);

use constant +{
    HAVE_ZLIB => eval { require Compress::Zlib; 1 },
    F_STORABLE => 1,
    F_COMPRESS => 2,
    COMPRESS_SAVINGS => 0.20,
};

our $VERSION = '0.00003';

sub new {
    my $class = shift;
    my $self  = bless {
        compess_threshold => 10_000,
        hashing_algorithm => undef,
        hashing_algorithm_class => 'Modulo',
        protocol => undef,
        protocol_class => 'Text',
        servers => undef,
        namespace => undef,
        @_,
        _is_connected => undef,
        _is_connecting => undef,
        _queue => [],
        _server_handles => undef,
    }, $class;

    $self->{protocol} ||= $self->_build_protocol;
    $self->{hashing_algorithm} ||= $self->_build_hashing_algorithm;

    return $self;
}

sub _build_hashing_algorithm {
    my $self = shift;
    my $h_class = $self->{hashing_algorithm_class};
    if ($h_class !~ s/^\+//) {
        $h_class = "Cache::Memcached::AnyEvent::Hash::$h_class";
    }

    $h_class =~ s/[^\w:_]//g; # cleanse

    eval "require $h_class";
    confess if $@;
    return $h_class->new();
}

sub _build_protocol {
    my $self = shift;
    my $p_class = $self->{protocol_class};
    if ($p_class !~ s/^\+//) {
        $p_class = "Cache::Memcached::AnyEvent::Protocol::$p_class";
    }

    $p_class =~ s/[^\w:_]//g; # cleanse

    eval "require $p_class";
    confess $@ if $@;
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
        confess if $@;
    }

    foreach my $attr qw(protocol hashing_algorithm) {
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
        confess if $@;
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
        confess if $@;
    }
}

sub connect {
    my $self = shift;

    return if $self->{_is_connecting} || $self->{_is_connected};

    $self->{_is_connecting} = 1;

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

        my $guard; $guard = tcp_connect $host, $port, sub {
            my ($fh, $host, $port) = @_;
            undef $guard; # thanks, buddy

            my $h; $h = AnyEvent::Handle->new(
                fh => $fh,
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

            $handles{ $server } = $h;
            $self->protocol->prepare_handle( $fh );
            $connect_cv->end;
        };
    }
}

sub add {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $exptime, $noreply) = @args;
    $self->push_queue( $self->protocol->{add_cb}, $self->protocol, $self, $key, $value, $exptime, $noreply, $cb );
}

sub decr {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $initial) = @args;
    $self->push_queue( $self->protocol->{decr_cb}, $self->protocol, $self, $key, $value, $initial, $cb );
}

sub delete {
    my ($self, @args) = @_;
    my $cb       = pop @args if ref $args[-1] eq 'CODE';
    my $noreply  = !defined $cb;
    $self->push_queue( $self->protocol->{delete_cb}, $self->protocol, $self, @args, $noreply, $cb );
}

sub get {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    $self->push_queue( $self->protocol->{get_multi_cb}, $self->protocol, $self, 'single', \@args, $cb );
}

sub get_handle { shift->{_server_handles}->{ $_[0] } }

sub get_multi {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    $self->push_queue( $self->protocol->{get_multi_cb}, $self->protocol, $self, 'multi', \@args, $cb );
}

sub get_server { shift->{servers}->[ $_[0] ] }
sub get_server_count { scalar @{ shift->{servers} } }

sub incr {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $initial) = @args;
    $self->push_queue( $self->protocol->{incr_cb}, $self->protocol, $self, $key, $value, $initial, $cb );
}

sub replace {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $exptime, $noreply) = @args;
    $self->push_queue( $self->protocol->{replace_cb}, $self->protocol, $self, $key, $value, $exptime, $noreply, $cb );
}

sub set {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($key, $value, $exptime, $noreply) = @args;
    $self->push_queue( $self->protocol->{set_cb}, $self->protocol, $self, $key, $value, $exptime, $noreply, $cb);
}

sub stats {
    my ($self, @args) = @_;
    my $cb = pop @args if ref $args[-1] eq 'CODE';
    my ($name) = @args;
    $self->push_queue( $self->protocol->{stats_cb}, $self->protocol, $self, $name, $cb );
}

sub version {
    my ($self, $cb) = @_;
    $self->push_queue( $self->protocol->{version_cb}, $self->protocol, $self, $cb);
}

sub push_queue {
    my ($self, $cb, @args) = @_;
    confess "no callback given" unless $cb;
    push @{$self->{queue}}, [ $cb, @args ];
    $self->drain_queue;
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
        my ($cb, @args) = @$next;
        $self->{_is_draining}++;
        $cb->(AnyEvent::Util::guard {
            my $t; $t = AE::timer 0, 0, sub {
                $self->{_is_draining}--;
                undef $t;
                $self->drain_queue;
            };
        }, @args);
    }
}

sub disconnect {
    my $self = shift;

    my $handles = delete $self->{_server_handles};
    foreach my $handle ( values %$handles ) {
        if ($handle) {
            $handle->push_shutdown();
            $handle->destroy();
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
    my $servers   = $self->{servers};
    my $count     = scalar @$servers;
    my $hash      = $self->hashing_algorithm->hash($key);
    my $i         = $hash % $count;
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
        $self->compress_enabled &&
        $len >= $threshold
    ;
    if ($compressable) {
        my $c_val = Compress::Zlib::memGzip($value);
        my $c_len = length($c_val);

        if ($c_len < $len * ( 1 - COMPRESS_SAVINGS() ) ) {
            $value = $c_val;
            $len = $c_len;
            $flags |= F_COMPRESS();
        }
    }
    $exptime = int($exptime || 0);

    return ($value, $len, $flags, $exptime);
}

1;

__END__

=head1 NAME

Cache::Memcached::AnyEvent - AnyEvent Compatible Memcached Client 

=head1 SYNOPSIS

    use Cache::Memcached::AnyEvent;

    my $memd = Cache::Memcached::AnyEvent->new(
        servers => [ '127.0.0.1:11211' ],
        compress_threshold => 10_000,
        namespace => 'myapp.',
    );

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

=head2 get_server($i)

=head2 get_server_count()

=head2 hashing_algorithm($object)

=head2 hashing_algorithm_class($class)

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

=head1 AUTHOR

Daisuke Maki C<< <daisuke@endeworks.jp> >>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut