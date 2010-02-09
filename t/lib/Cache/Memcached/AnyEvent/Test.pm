package Cache::Memcached::AnyEvent::Test;
use strict;
use Cache::Memcached::AnyEvent;
use IO::Socket::INET;
use Test::More;
use base qw(Exporter);

our @EXPORT = qw(test_client);

sub import {
    my $class = shift;
    Test::More->export_to_level(1, @_);
    $class->Exporter::export_to_level(1, @_);
}

sub test_servers {
    my $servers = $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS};
    $servers ||= 'localhost:11211';
    return split(/\s*,\s*/, $servers);
}

sub test_client {
    my @servers;
    foreach my $server ( test_servers() ) {
        my ($host, $port) = split(/:/, $server);
        my $socket = IO::Socket::INET->new(
            PeerHost => $host,
            PeerPort => $port,
        );
        if ($socket) {
            push @servers, $server;
        }
    }

    if (! @servers) {
        plan skip_all => "Can't talk to any memcached servers";
    }

    return Cache::Memcached::AnyEvent->new(
        servers => \@servers,
    );
}
    

1;
