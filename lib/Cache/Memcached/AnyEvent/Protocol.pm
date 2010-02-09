package Cache::Memcached::AnyEvent::Protocol;
use strict;

use constant +{
    HAVE_ZLIB => eval { require Compress::Zlib; 1 },
    F_STORABLE => 1,
    F_COMPRESS => 2,
    COMPRESS_SAVINGS => 0.20,
};

sub new {
    my $class = shift;
    my $self  = bless {@_}, $class;

    foreach my $cb qw(add_cb decr_cb delete_cb incr_cb get_cb get_multi_cb replace_cb set_cb stats_cb version_cb) {
        my $method = "_build_$cb";
        $self->{$cb} ||= $self->$method;
    }
    return $self;
}

sub _build_add_cb {}
sub _build_decr_cb {}
sub _build_delete_cb {}
sub _build_incr_cb {}
sub _build_get_cb {}
sub _build_get_multi_cb {}
sub _build_replace_cb {}
sub _build_set_cb {}
sub _build_stats_cb {}
sub _build_version_cb {}

sub prepare_handle {}

sub get_handle_for {
    my ($self, $key) = @_;
    my $memcached = $self->{memcached};
    my $count     = $memcached->get_server_count();
    my $hash      = $memcached->hashing_algorithm->hash($key);
    my $i         = $hash % $count;
    my $handle    = $memcached->get_handle( $memcached->get_server($i) );
    if (! $handle) {
        die "Could not find handle for $key";
    }

    return $handle;
}

sub prepare_key {
    my ($self, $key) = @_;
    if (my $ns = $self->{memcached}->namespace) {
        $key = $ns . $key;
    }
    return $key;
}

sub prepare_value {
    my ($self, $cmd, $value, $exptime) = @_;

    my $memcached = $self->{memcached};

    my $flags = 0;
    if (ref $value) {
        $value = Storable::nfreeze($value);
        $flags |= F_STORABLE();
    }

    my $len = bytes::length($value);
    my $threshold = $memcached->compress_threshold;
    my $compressable = 
        ($cmd ne 'append' && $cmd ne 'prepend') &&
        $threshold && 
        HAVE_ZLIB() &&
        $memcached->compress_enabled &&
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

sub decode_key {
    my ($self, $key) = @_;

    if (my $ns = $self->{memcached}->namespace) {
        $key =~ s/^$ns//;
    }
    return $key;
}

sub decode_value {
    my ($self, $flags, $data) = @_;
    if ($flags & F_COMPRESS() && HAVE_ZLIB()) {
        $data = Compress::Zlib::memGunzip($data);
    }
    if ($flags & F_STORABLE()) {
        $data = Storable::thaw($data);
    }
    return $data;
}

1;

__END__

=head1 NAME

Cache::Memcached::AnyEvent::Protocol - Base Class For Memcached Protocol

=head1 SYNOPSIS

    package NewProtocol;
    use strict;
    use base 'Cache::Memcached::AnyEvent::Protocol';

=cut