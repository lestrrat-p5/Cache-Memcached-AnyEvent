package Cache::Memcached::AnyEvent::Hash::Modulo;
use strict;
use String::CRC32 qw(crc32);

sub new {
    my $class = shift;
    bless {@_}, $class;
}

sub hash {
    return (crc32($_[1]) >> 16) & 0x7fff;
}

1;