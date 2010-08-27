use strict;
use Test::More;
use t::Cache::Memcached::AnyEvent::Test;

my $memd = test_client() or exit;

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
done_testing;
