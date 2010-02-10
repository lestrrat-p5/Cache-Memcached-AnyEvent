package Cache::Memcached::AnyEvent::Hash::Modulo;
use strict;
use String::CRC32 qw(crc32);

sub new {
    my $class = shift;
    bless {@_}, $class;
}

sub hash {
    my ($self, $key, $memcached) = @_;
    my $count = $memcached->{_active_server_count};
    return ((crc32($key) >> 16) & 0x7fff) % $count;
}

1;

__END__

=head1 NAME

Cache::Memached::AnyEvent::Hash::Modulo - Default Key Hashing Strategy

=cut