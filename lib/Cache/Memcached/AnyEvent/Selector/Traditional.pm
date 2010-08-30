package Cache::Memcached::AnyEvent::Selector::Traditional;
use strict;
use Carp qw(croak);
use String::CRC32 qw(crc32);

sub new {
    my $class = shift;
    bless { @_, buckets => [], bucketcount => 0 }, $class;
}

sub add_server {
    my ($self, $server, $h) = @_;

    my %b2h = map { ( $_ => $self->hashkey($_) ) } @{ $self->{buckets} };
    $b2h{ $server } = $self->hashkey($server);

    my @newbuckets = sort { $b2h{ $a } <=> $b2h{ $b } } keys %b2h;
    $self->{buckets} = \@newbuckets;
    $self->{bucketcount} = scalar @newbuckets;
    ();
}

# for object definition's sake, it's good to make this a method
# but for efficiency, this ->hashkey call is just stupid
sub hashkey {
    return (crc32($_[1]) >> 16) & 0x7ffff;
}

sub get_handle {
    my ($self, $key) = @_;

    my $count = $self->{bucketcount};
    if ($count > 0) {
        my $handles = $self->{memcached}->{_server_handles};

        # short-circuit for when there's only one socket
        if ($count == 1) {
            return (values %$handles)[0];
        }
    
        # if there are multiple servers, choose the right one
        my $hv = $self->hashkey( $key );
        my $buckets = $self->{buckets};
        for my $i (1..20) {
            my $handle = $handles->{ $buckets->[ $hv % $count ] };
            if ($handle) {
                return $handle;
            }
        
            $hv += $self->hashkey( $i . $key );
        }
    }

    croak "Could not find a suitable handle for key $key";
}

1;

__END__

=head1 NAME

Cache::Memcached::AnyEvent::Selector::Traditional - Traditional Server Selection Algorithm (ala Cache::Memcached)

=head1 SYNOPSIS

    use Cache::Memcached::AnyEvent;
    my $memd = Cache::Memcached::AnyEvent->new({
        ...
        selector_class => 'Traditional', # Default, so you can omit
    });

=head1 DESCRIPTION

Implements the default server selection mechanism, as implemented by
Cache::Memcached. You do not need to explicitly specify this, as this is
the default selector.

=head1 METHODS

=head2 $class->new( memcached => $memd )

Constructor.

=head2 $selector->add_server( $server, $handle )

Called when a new server connection is made.

=head2 $handle = $selector->get_handle( $key )

Returns the AnyEvent handle that is responsible for handling C<$key>

=head2 $hash = $selector->hashkey( $key )

Returns the hash value for key C<$key>

=cut