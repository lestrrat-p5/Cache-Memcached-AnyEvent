use strict;
use lib "t/lib";
use Test::More;
use Cache::Memcached::AnyEvent::Test;

my $memd = test_client() or exit;
plan tests => 2;

my $cv = AE::cv;

foreach my $protocol qw(Text Binary) {
    SKIP: {
        if ($protocol eq 'Binary') {
            skip "stats() for Binary protocol unimplemented", 1;
        }
        $memd->protocol_class($protocol);
        $cv->begin;
        $memd->stats( sub {
            my $stats = shift;

            foreach my $server ( @{ $memd->servers } ) {
                is( ref $stats->{$server}, 'HASH', "Stats for $server exists" );
            }
            $cv->end;
        } );
    }
}

$cv->recv;
