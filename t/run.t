use strict;
use Test::More;
use Test::Memcached;
use Test::Requires;
use t::CMAETest::Consistency;
use t::CMAETest::Commands;
use t::CMAETest::ConnectFail;
use t::CMAETest::CV;
use t::CMAETest::Dorman;
use t::CMAETest::Stats;

my @memd;
if ( ! $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS}) {
    my $port;
    for (1..5) {
        my $memd = Test::Memcached->new(base_dir => 't', options => { verbose => 1 });
            
        if (! $memd) {
            plan skip_all => "Failed to start memcached server";
        }
        if ($port) {
            $memd->start( tcp_port => $port );
        } else {
            $memd->start();
        }

        if ($port) {
            $port++;
        } else {
            $port = $memd->option('tcp_port') + 1;
        }

        # give it a second for the server to start
        push @memd, $memd;
    }

    $ENV{PERL_ANYEVENT_MEMCACHED_SERVERS} = join(',', 
        map { sprintf('127.0.0.1:%d', $_->option('tcp_port')) } @memd
    );
}

foreach my $protocol qw(Text Binary) {
    foreach my $selector qw(Traditional Ketama) {
        foreach my $pkg qw( t::CMAETest::Commands t::CMAETest::ConnectFail t::CMAETest::CV t::CMAETest::Dorman t::CMAETest::Stats t::CMAETest::Consistency) {
            note "running $pkg test [$protocol/$selector]";
            subtest "$pkg [$protocol/$selector]" => sub {
                if ( $selector eq 'Ketama' ) {
                    Test::Requires->import( 'Algorithm::ConsistentHash::Ketama' );
                }
                $pkg->run( $protocol, $selector );
            };
        }
    }
}

done_testing();