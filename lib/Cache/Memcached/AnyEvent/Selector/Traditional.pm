package Cache::Memcached::AnyEvent::Selector::Traditional;
use strict;
use Carp qw(croak);
use String::CRC32 qw(crc32);

sub new {
    my $class = shift;
    bless { @_ }, $class;
}

sub add_server {} # nothing to do, as long as we have memcached ref

# for object definition's sake, it's good to make this a method
# but for efficiency, this ->hashkey call is just stupid
sub hashkey {
    return (crc32($_[1]) >> 16) & 0x7ffff;
}

sub get_handle {
    my ($self, $key) = @_;

    my $count = $self->{memcached}->{_active_server_count};
    if ($count > 0) {
        my $servers = $self->{memcached}->{_active_servers};
        my $handles = $self->{memcached}->{_server_handles};

        # short-circuit for when there's only one socket
        if ($count == 1) {
            return (values %$handles)[0];
        }
    
        # if there are multiple servers, choose the right one
        my $hv = $self->hashkey( $key );
        for my $i (1..20) {
            my $handle = $handles->{ $servers->[ $hv % $count ] };
            if ($handle) {
                return $handle;
            }
        
            $hv += $self->hashkey( $i . $key );
        }
    }

    croak "Could not find a suitable handle for key $key";
}

1;